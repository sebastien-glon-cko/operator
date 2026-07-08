package build

import (
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/utils/ptr"

	vmv1beta1 "github.com/VictoriaMetrics/operator/api/operator/v1beta1"
)

type scrapeBuilder interface {
	GetServiceScrape() *vmv1beta1.VMServiceScrapeSpec
	GetExtraArgs() map[string]string
	GetMetricsPath() string
	UseTLS() bool
}

type podScrapeBuilder interface {
	scrapeBuilder
	GetNamespace() string
	PrefixedName() string
	SelectorLabels() map[string]string
	AsOwner() metav1.OwnerReference
}

// primaryPortNamer is implemented by CRDs supporting multiple HTTPListeners,
// returning the Service port name generated for the primary listener.
type primaryPortNamer interface {
	PrimaryPortName() string
}

// listenerLookup is implemented by CRDs supporting multiple HTTPListeners.
type listenerLookup interface {
	GetListener(name string) *vmv1beta1.HTTPListener
}

// VMServiceScrape creates corresponding object with `http` port endpoint obtained from given service
// add additionalPortNames to the monitoring if needed
func VMServiceScrape(service *corev1.Service, b scrapeBuilder, additionalPortNames ...string) *vmv1beta1.VMServiceScrape {
	var endpoints []vmv1beta1.Endpoint

	extraArgs := b.GetExtraArgs()
	authKey := extraArgs[vmv1beta1.MetricsAuthKeyFlag]

	defaultPortName := "http"
	if pn, ok := b.(primaryPortNamer); ok {
		defaultPortName = pn.PrimaryPortName()
	}
	ll, hasListeners := b.(listenerLookup)

	for _, servicePort := range service.Spec.Ports {
		var listener *vmv1beta1.HTTPListener
		if hasListeners {
			listener = ll.GetListener(servicePort.Name)
		}
		if listener != nil && ptr.Deref(listener.UseProxyProtocol, false) {
			continue
		}

		// fast path - filter all unmatched ports
		if servicePort.Name != defaultPortName && listener == nil && len(additionalPortNames) == 0 {
			continue
		}

		var extraRelabelingRules vmv1beta1.EndpointRelabelings
		path := b.GetMetricsPath()
		if servicePort.Name != defaultPortName && listener == nil {
			// check service for extra ports
			var nameMatched bool
			for _, filter := range additionalPortNames {
				if servicePort.Name == filter {
					nameMatched = true
					// sidecars (config-reloader, vmbackupmanager) always expose metrics at the
					// literal path below, regardless of the app's own http.pathPrefix
					path = "/metrics"
					// add a relabeling rule to avoid job collision
					extraRelabelingRules.RelabelConfigs = []*vmv1beta1.RelabelConfig{
						{
							SourceLabels: []string{"job"},
							TargetLabel:  "job",
							Regex:        vmv1beta1.StringOrArray{"(.+)"},
							Replacement:  ptr.To("${1}-" + filter),
						},
					}
					break
				}
			}
			if !nameMatched {
				continue
			}
		}

		endpoint := vmv1beta1.Endpoint{
			Port:                servicePort.Name,
			EndpointRelabelings: extraRelabelingRules,
			EndpointScrapeParams: vmv1beta1.EndpointScrapeParams{
				Path: path,
			},
		}
		useTLS := b.UseTLS()
		if listener != nil {
			useTLS = listener.UseTLS(vmv1beta1.UseTLS(extraArgs))
		}
		if useTLS {
			endpoint.Scheme = "https"
			// add insecure by default
			// if needed user will override it with direct config
			endpoint.TLSConfig = &vmv1beta1.TLSConfig{
				InsecureSkipVerify: true,
			}
		}
		if len(authKey) > 0 {
			endpoint.Params = map[string][]string{
				"authKey": {authKey},
			}
		}
		endpoints = append(endpoints, endpoint)
	}

	serviceScrapeSpec := b.GetServiceScrape()
	if serviceScrapeSpec == nil {
		serviceScrapeSpec = &vmv1beta1.VMServiceScrapeSpec{}
	}
	scrape := &vmv1beta1.VMServiceScrape{
		ObjectMeta: metav1.ObjectMeta{
			Name:            service.Name,
			Namespace:       service.Namespace,
			OwnerReferences: service.OwnerReferences,
			Labels:          service.Labels,
			Annotations:     service.Annotations,
		},
		Spec: *serviceScrapeSpec,
	}
	// merge generated endpoints into user defined values by Port name
	// assume, that it must be unique.
	for _, e := range endpoints {
		var found bool
		for idx := range scrape.Spec.Endpoints {
			eps := &scrape.Spec.Endpoints[idx]
			if eps.Port == e.Port {
				found = true
				if eps.Path == "" {
					eps.Path = e.Path
				}
			}
		}
		if !found {
			scrape.Spec.Endpoints = append(scrape.Spec.Endpoints, e)
		}
	}
	// allow to manually define selectors
	// in some cases it may be useful
	// for instance when additional service created with extra pod ports
	if scrape.Spec.Selector.MatchLabels == nil && scrape.Spec.Selector.MatchExpressions == nil {
		scrape.Spec.Selector = metav1.LabelSelector{
			MatchLabels: service.Labels,
			MatchExpressions: []metav1.LabelSelectorRequirement{
				{Key: vmv1beta1.AdditionalServiceLabel, Operator: metav1.LabelSelectorOpDoesNotExist},
			},
		}
	}
	for i := range scrape.Spec.Endpoints {
		addVictoriaMetricsAppRelabelConfig(&scrape.Spec.Endpoints[i].EndpointRelabelings)
	}

	return scrape
}

// listenersEnumerator is implemented by CRDs supporting multiple HTTPListeners.
type listenersEnumerator interface {
	GetListeners() []vmv1beta1.HTTPListener
}

// VMPodScrape builds a VMPodScrape for given podScrapeBuilder, with portName as the primary
// endpoint (or one endpoint per listener, if b supports multiple) and any additionalPortNames
// (e.g. sidecar metrics ports) appended alongside it.
func VMPodScrape(b podScrapeBuilder, portName string, additionalPortNames ...string) *vmv1beta1.VMPodScrape {
	extraArgs := b.GetExtraArgs()
	authKey := extraArgs[vmv1beta1.MetricsAuthKeyFlag]

	buildEndpoint := func(name string, useTLS, isPrimary bool) vmv1beta1.PodMetricsEndpoint {
		path := b.GetMetricsPath()
		var relabelings vmv1beta1.EndpointRelabelings
		if !isPrimary {
			// sidecars (e.g. config-reloader) always expose metrics at the literal path
			// below, regardless of the app's own http.pathPrefix
			path = "/metrics"
			relabelings.RelabelConfigs = []*vmv1beta1.RelabelConfig{
				{
					SourceLabels: []string{"job"},
					TargetLabel:  "job",
					Regex:        vmv1beta1.StringOrArray{"(.+)"},
					Replacement:  ptr.To("${1}-" + name),
				},
			}
		}
		ep := vmv1beta1.PodMetricsEndpoint{
			Port:                ptr.To(name),
			EndpointRelabelings: relabelings,
			EndpointScrapeParams: vmv1beta1.EndpointScrapeParams{
				Path: path,
			},
		}
		if useTLS {
			ep.Scheme = "https"
			// add insecure by default
			// if needed user will override it with direct config
			ep.TLSConfig = &vmv1beta1.TLSConfig{
				InsecureSkipVerify: true,
			}
		}
		if len(authKey) > 0 {
			ep.Params = map[string][]string{
				"authKey": {authKey},
			}
		}
		return ep
	}

	var endpoints []vmv1beta1.PodMetricsEndpoint
	if le, ok := b.(listenersEnumerator); ok {
		useTLS := vmv1beta1.UseTLS(extraArgs)
		useProxyProtocol := vmv1beta1.UseProxyProtocol(extraArgs)
		for _, l := range le.GetListeners() {
			if ptr.Deref(l.UseProxyProtocol, useProxyProtocol) {
				continue
			}
			endpoints = append(endpoints, buildEndpoint(l.Name, l.UseTLS(useTLS), true))
		}
	}
	if len(endpoints) == 0 {
		endpoints = append(endpoints, buildEndpoint(portName, b.UseTLS(), true))
	}
	for _, name := range additionalPortNames {
		endpoints = append(endpoints, buildEndpoint(name, b.UseTLS(), false))
	}

	selectorLabels := b.SelectorLabels()
	scrape := &vmv1beta1.VMPodScrape{
		ObjectMeta: metav1.ObjectMeta{
			Name:            b.PrefixedName(),
			Namespace:       b.GetNamespace(),
			Labels:          selectorLabels,
			OwnerReferences: []metav1.OwnerReference{b.AsOwner()},
		},
		Spec: vmv1beta1.VMPodScrapeSpec{
			Selector:            *metav1.SetAsLabelSelector(selectorLabels),
			PodMetricsEndpoints: endpoints,
		},
	}
	serviceScrapeSpec := b.GetServiceScrape()
	if serviceScrapeSpec != nil {
		for _, e := range serviceScrapeSpec.Endpoints {
			var found bool
			for idx := range scrape.Spec.PodMetricsEndpoints {
				pep := &scrape.Spec.PodMetricsEndpoints[idx]
				if pep.Port != nil && *pep.Port == e.Port {
					found = true
					pep.EndpointScrapeParams = e.EndpointScrapeParams
					pep.EndpointRelabelings = e.EndpointRelabelings
					break
				}
			}
			if !found {
				scrape.Spec.PodMetricsEndpoints = append(scrape.Spec.PodMetricsEndpoints, vmv1beta1.PodMetricsEndpoint{
					Port:                 ptr.To(e.Port),
					EndpointRelabelings:  e.EndpointRelabelings,
					EndpointScrapeParams: e.EndpointScrapeParams,
				})
			}
		}
		scrape.Spec.PodTargetLabels = serviceScrapeSpec.PodTargetLabels
		scrape.Spec.SampleLimit = serviceScrapeSpec.SampleLimit
		scrape.Spec.SeriesLimit = serviceScrapeSpec.SeriesLimit
		scrape.Spec.AttachMetadata = serviceScrapeSpec.AttachMetadata
	}
	for i := range scrape.Spec.PodMetricsEndpoints {
		addVictoriaMetricsAppRelabelConfig(&scrape.Spec.PodMetricsEndpoints[i].EndpointRelabelings)
	}
	return scrape
}

func addVictoriaMetricsAppRelabelConfig(relabelings *vmv1beta1.EndpointRelabelings) {
	for _, rc := range relabelings.RelabelConfigs {
		if rc != nil && (rc.TargetLabel == "victoriametrics_app" || rc.UnderScoreTargetLabel == "victoriametrics_app") {
			return
		}
	}
	relabelings.RelabelConfigs = append(relabelings.RelabelConfigs, victoriaMetricsAppRelabelConfig())
}

func victoriaMetricsAppRelabelConfig() *vmv1beta1.RelabelConfig {
	return &vmv1beta1.RelabelConfig{
		TargetLabel: "victoriametrics_app",
		Replacement: ptr.To("true"),
	}
}
