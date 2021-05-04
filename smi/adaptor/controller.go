package adaptor

import (
	"context"
	"fmt"
	"time"

	serviceprofile "github.com/linkerd/linkerd2/controller/gen/apis/serviceprofile/v1alpha2"
	spclientset "github.com/linkerd/linkerd2/controller/gen/client/clientset/versioned"
	spinformers "github.com/linkerd/linkerd2/controller/gen/client/informers/externalversions/serviceprofile/v1alpha2"
	splisters "github.com/linkerd/linkerd2/controller/gen/client/listers/serviceprofile/v1alpha2"
	trafficsplit "github.com/servicemeshinterface/smi-sdk-go/pkg/apis/split/v1alpha1"
	tsclientset "github.com/servicemeshinterface/smi-sdk-go/pkg/gen/client/split/clientset/versioned"
	informers "github.com/servicemeshinterface/smi-sdk-go/pkg/gen/client/split/informers/externalversions/split/v1alpha1"
	listers "github.com/servicemeshinterface/smi-sdk-go/pkg/gen/client/split/listers/split/v1alpha1"
	log "github.com/sirupsen/logrus"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/selection"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/util/workqueue"
)

const agent = "smi-controller"

// SMIController is an adaptor that converts SMI resources
// into Linkerd primitive resources
type SMIController struct {
	kubeclientset kubernetes.Interface
	clusterDomain string

	// TrafficSplit clientset, informers and Listers
	tsclientset tsclientset.Interface
	tsLister    listers.TrafficSplitLister
	tsSynced    cache.InformerSynced

	// Service Profile Lister
	spLister    splisters.ServiceProfileLister
	spclientset spclientset.Interface

	// workqueue is a rate limited work queue. This is used to queue work to be
	// processed instead of performing it as soon as a change happens.
	workqueue workqueue.RateLimitingInterface
}

// NewController returns a new sample controller
func NewController(
	kubeclientset kubernetes.Interface,
	clusterDomain string,
	tsclientset tsclientset.Interface,
	spclientset spclientset.Interface,
	spInformer spinformers.ServiceProfileInformer,
	tsInformer informers.TrafficSplitInformer) *SMIController {

	controller := &SMIController{
		kubeclientset: kubeclientset,
		clusterDomain: clusterDomain,
		tsclientset:   tsclientset,
		tsLister:      tsInformer.Lister(),
		tsSynced:      tsInformer.Informer().HasSynced,
		spclientset:   spclientset,
		spLister:      spInformer.Lister(),
		workqueue:     workqueue.NewNamedRateLimitingQueue(workqueue.DefaultControllerRateLimiter(), "TrafficSplits"),
	}

	// Set up an event handler for when Ts resources change
	tsInformer.Informer().AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc: controller.enqueueTs,
		UpdateFunc: func(old, new interface{}) {
			controller.enqueueTs(new)
		},
		// DeleteFunc: controller., ???
	})

	return controller
}

// Run will set up the event handlers for types we are interested in, as well
// as syncing informer caches and starting workers. It will block until stopCh
// is closed, at which point it will shutdown the workqueue and wait for
// workers to finish processing their current work items.
func (c *SMIController) Run(stopCh <-chan struct{}) error {
	defer utilruntime.HandleCrash()
	defer c.workqueue.ShutDown()

	// Start the informer factories to begin populating the informer caches
	log.Info("Starting Ts controller")

	// Wait for the caches to be synced before starting workers
	log.Info("Waiting for informer caches to sync")
	if ok := cache.WaitForCacheSync(stopCh, c.tsSynced); !ok {
		return fmt.Errorf("failed to wait for caches to sync")
	}

	log.Info("Starting workers")
	// Launch  workers to process TS resources
	go wait.Until(c.runWorker, time.Second, stopCh)

	log.Info("Started workers")
	<-stopCh
	log.Info("Shutting down workers")

	return nil
}

// runWorker is a long-running function that will continually call the
// processNextWorkItem function in order to read and process a message on the
// workqueue.
func (c *SMIController) runWorker() {
	for c.processNextWorkItem() {
	}
}

// processNextWorkItem will read a single work item off the workqueue and
// attempt to process it, by calling the syncHandler.
func (c *SMIController) processNextWorkItem() bool {
	obj, shutdown := c.workqueue.Get()

	if shutdown {
		return false
	}

	// We wrap this block in a func so we can defer c.workqueue.Done.
	err := func(obj interface{}) error {
		// We call Done here so the workqueue knows we have finished
		// processing this item. We also must remember to call Forget if we
		// do not want this work item being re-queued. For example, we do
		// not call Forget if a transient error occurs, instead the item is
		// put back on the workqueue and attempted again after a back-off
		// period.
		defer c.workqueue.Done(obj)
		var key string
		var ok bool
		// We expect strings to come off the workqueue. These are of the
		// form namespace/name. We do this as the delayed nature of the
		// workqueue means the items in the informer cache may actually be
		// more up to date that when the item was initially put onto the
		// workqueue.
		if key, ok = obj.(string); !ok {
			// As the item in the workqueue is actually invalid, we call
			// Forget here else we'd go into a loop of attempting to
			// process a work item that is invalid.
			c.workqueue.Forget(obj)
			utilruntime.HandleError(fmt.Errorf("expected string in workqueue but got %#v", obj))
			return nil
		}
		// Run the syncHandler, passing it the namespace/name string of the
		// Ts resource to be synced.
		if err := c.syncHandler(key); err != nil {
			// Put the item back on the workqueue to handle any transient errors.
			c.workqueue.AddRateLimited(key)
			return fmt.Errorf("error syncing '%s': %s, requeuing", key, err.Error())
		}
		// Finally, if no error occurs we Forget this item so it does not
		// get queued again until another change happens.
		c.workqueue.Forget(obj)
		log.Infof("Successfully synced '%s'", key)
		return nil
	}(obj)

	if err != nil {
		utilruntime.HandleError(err)
		return true
	}

	return true
}

// syncHandler compares the actual state with the desired, and attempts to
// converge the two. It then updates the Status block of the Ts resource
// with the current status of the resource.
func (c *SMIController) syncHandler(key string) error {
	// Convert the namespace/name string into a distinct namespace and name
	namespace, name, err := cache.SplitMetaNamespaceKey(key)
	if err != nil {
		utilruntime.HandleError(fmt.Errorf("invalid resource key: %s", key))
		return nil
	}

	// Get the Ts resource with this namespace/name
	ts, err := c.tsLister.TrafficSplits(namespace).Get(name)
	if err != nil {
		// The Ts resource may no longer exist, in which case we stop
		// processing.
		if errors.IsNotFound(err) {
			utilruntime.HandleError(fmt.Errorf("ts '%s' in work queue no longer exists", key))
			return nil
		}

		return err
	}

	matchLabels := map[string]string{
		"created-by": agent,
		"parent-ts":  ts.Name,
	}
	selector := labels.NewSelector()
	for k, v := range matchLabels {
		r, err := labels.NewRequirement(k, selection.Equals, []string{v})
		if err != nil {
			return err
		}
		selector.Add(*r)
	}

	// Check if the Service Profile is already Present
	serviceProfiles, err := c.spLister.ServiceProfiles(ts.Namespace).List(selector)
	if err != nil {
		return err
	}

	var sp *serviceprofile.ServiceProfile
	if len(serviceProfiles) == 0 {
		// Create a Service Profile resource if not exist
		sp, err = c.spclientset.LinkerdV1alpha2().ServiceProfiles(ts.Namespace).Create(context.Background(), c.toServiceProfile(ts), metav1.CreateOptions{})
		if err != nil {
			return err
		}
	} else {
		sp = serviceProfiles[0]
	}

	// If the Serviceprofile is not controlled by this Ts resource, we should log
	// a warning to the event recorder and return error msg.
	if !metav1.IsControlledBy(sp, ts) {
		return fmt.Errorf("sp resource %s already exists but not controller by the adaptor", sp.Name)
	}

	// Check if SP Matches the TS, and update if it not ???

	return nil
}

// enqueueTs takes a Ts resource and converts it into a namespace/name
// string which is then put onto the work queue. This method should *not* be
// passed resources of any type other than Ts.
func (c *SMIController) enqueueTs(obj interface{}) {
	var key string
	var err error
	if key, err = cache.MetaNamespaceKeyFunc(obj); err != nil {
		utilruntime.HandleError(err)
		return
	}
	c.workqueue.Add(key)
}

func (c *SMIController) toServiceProfile(ts *trafficsplit.TrafficSplit) *serviceprofile.ServiceProfile {
	spResource := serviceprofile.ServiceProfile{
		ObjectMeta: metav1.ObjectMeta{
			Name:      fmt.Sprintf("%s.%s.svc.%s", ts.Spec.Service, ts.Namespace, c.clusterDomain),
			Namespace: ts.Namespace,
			Labels: map[string]string{
				"created-by": agent,
				"parent-ts":  ts.Name,
			},
			// Add More Metadata ???
			OwnerReferences: []metav1.OwnerReference{
				*metav1.NewControllerRef(ts, trafficsplit.SchemeGroupVersion.WithKind("TrafficSplit")),
			},
		},
		Spec: serviceprofile.ServiceProfileSpec{
			Routes: []*serviceprofile.RouteSpec{},
		},
	}

	for _, backend := range ts.Spec.Backends {
		weightedDst := &serviceprofile.WeightedDst{
			Authority: fmt.Sprintf("%s.%s.svc.%s", backend.Service, ts.Namespace, c.clusterDomain),
			Weight:    *backend.Weight,
		}

		spResource.Spec.DstOverrides = append(spResource.Spec.DstOverrides, weightedDst)
	}

	return &spResource
}
