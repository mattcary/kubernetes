/*
Copyright 2016 The Kubernetes Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package statefulset

import (
	"errors"
	"fmt"
	"math/rand"
	"reflect"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"

	apps "k8s.io/api/apps/v1"
	v1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/types"
	utilerrors "k8s.io/apimachinery/pkg/util/errors"
	utilfeature "k8s.io/apiserver/pkg/util/feature"
	"k8s.io/client-go/informers"
	appsinformers "k8s.io/client-go/informers/apps/v1"
	coreinformers "k8s.io/client-go/informers/core/v1"
	clientset "k8s.io/client-go/kubernetes"
	"k8s.io/client-go/kubernetes/fake"
	appslisters "k8s.io/client-go/listers/apps/v1"
	corelisters "k8s.io/client-go/listers/core/v1"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/tools/record"
	featuregatetesting "k8s.io/component-base/featuregate/testing"
	podutil "k8s.io/kubernetes/pkg/api/v1/pod"
	"k8s.io/kubernetes/pkg/controller"
	"k8s.io/kubernetes/pkg/controller/history"
	"k8s.io/kubernetes/pkg/features"
)

type invariantFunc func(set *apps.StatefulSet, om *fakeObjectManager) error

func setupController(client clientset.Interface) (*fakeObjectManager, *fakeStatefulSetStatusUpdater, StatefulSetControlInterface, chan struct{}) {
	informerFactory := informers.NewSharedInformerFactory(client, controller.NoResyncPeriodFunc())
	om := newFakeObjectManager(informerFactory.Core().V1().Pods(), informerFactory.Apps().V1().StatefulSets(), informerFactory.Apps().V1().ControllerRevisions())
	spc := NewStatefulPodControlFromManager(om, &noopRecorder{})
	ssu := newFakeStatefulSetStatusUpdater(informerFactory.Apps().V1().StatefulSets())
	recorder := &noopRecorder{}
	ssc := NewDefaultStatefulSetControl(spc, ssu, history.NewFakeHistory(informerFactory.Apps().V1().ControllerRevisions()), recorder)

	stop := make(chan struct{})
	informerFactory.Start(stop)
	cache.WaitForCacheSync(
		stop,
		informerFactory.Apps().V1().StatefulSets().Informer().HasSynced,
		informerFactory.Core().V1().Pods().Informer().HasSynced,
		informerFactory.Apps().V1().ControllerRevisions().Informer().HasSynced,
	)
	return om, ssu, ssc, stop
}

func burst(set *apps.StatefulSet) *apps.StatefulSet {
	set.Spec.PodManagementPolicy = apps.ParallelPodManagement
	return set
}


func setMinReadySeconds(set *apps.StatefulSet, minReadySeconds int32) *apps.StatefulSet {
	set.Spec.MinReadySeconds = minReadySeconds
	return set
}

func runTestOverPVCDeletePolicies(t *testing.T, testName string, testFn func(*testing.T, apps.PersistentVolumeClaimDeletePolicyType)) {
	subtestName := "StatefulSetAutoDeletePVCDisabled"
	if testName != "" {
		subtestName = fmt.Sprintf("%s/%s", testName, subtestName)
	}
	t.Run(subtestName, func(t *testing.T) {
		defer featuregatetesting.SetFeatureGateDuringTest(t, utilfeature.DefaultFeatureGate, features.StatefulSetAutoDeletePVC, false)()
		testFn(t, apps.RetainPersistentVolumeClaimDeletePolicyType)
	})

	for _, policy := range []apps.PersistentVolumeClaimDeletePolicyType{
		apps.RetainPersistentVolumeClaimDeletePolicyType,
		apps.DeleteOnStatefulSetDeletionOnlyPersistentVolumeClaimDeletePolicyType,
		apps.DeleteOnScaledownOnlyPersistentVolumeClaimDeletePolicyType,
		apps.DeleteOnScaledownAndStatefulSetDeletionPersistentVolumeClaimDeletePolicyType,
	} {
		subtestName := string(policy) + "/StatefulSetAutoDeletePVCEnabled"
		if testName != "" {
			subtestName = fmt.Sprintf("%s/%s", testName, subtestName)
		}
		t.Run(subtestName, func(t *testing.T) {
			defer featuregatetesting.SetFeatureGateDuringTest(t, utilfeature.DefaultFeatureGate, features.StatefulSetAutoDeletePVC, true)()
			testFn(t, policy)
		})
	}
}

func TestStatefulSetControl(t *testing.T) {
	simpleSetFn := func() *apps.StatefulSet { return newStatefulSet(3) }
	largeSetFn := func() *apps.StatefulSet { return newStatefulSet(5) }

	testCases := []struct {
		fn  func(*testing.T, *apps.StatefulSet, invariantFunc)
		obj func() *apps.StatefulSet
	}{
		{CreatesPods, simpleSetFn},
		{ScalesUp, simpleSetFn},
		{ScalesDown, simpleSetFn},
		{ReplacesPods, largeSetFn},
		{RecreatesFailedPod, simpleSetFn},
		{CreatePodFailure, simpleSetFn},
		{UpdatePodFailure, simpleSetFn},
		{UpdateSetStatusFailure, simpleSetFn},
		{PodRecreateDeleteFailure, simpleSetFn},
	}

	for _, testCase := range testCases {
		fnName := runtime.FuncForPC(reflect.ValueOf(testCase.fn).Pointer()).Name()
		if i := strings.LastIndex(fnName, "."); i != -1 {
			fnName = fnName[i+1:]
		}
		testObj := testCase.obj
		testFn := testCase.fn
		runTestOverPVCDeletePolicies(
			t,
			fmt.Sprintf("%s/Monotonic", fnName),
			func(t *testing.T, policy apps.PersistentVolumeClaimDeletePolicyType) {
				set := testObj()
				set.Spec.PersistentVolumeClaimDeletePolicy = &policy
				testFn(t, set, assertMonotonicInvariants)
			},
		)
		runTestOverPVCDeletePolicies(
			t,
			fmt.Sprintf("%s/Burst", fnName),
			func(t *testing.T, policy apps.PersistentVolumeClaimDeletePolicyType) {
				set := burst(testObj())
				set.Spec.PersistentVolumeClaimDeletePolicy = &policy
				testFn(t, set, assertBurstInvariants)
			},
		)
	}
}

func CreatesPods(t *testing.T, set *apps.StatefulSet, invariants invariantFunc) {
	client := fake.NewSimpleClientset(set)
	om, _, ssc, stop := setupController(client)
	defer close(stop)

	if err := scaleUpStatefulSetControl(set, ssc, om, invariants); err != nil {
		t.Errorf("Failed to turn up StatefulSet : %s", err)
	}
	var err error
	set, err = om.setsLister.StatefulSets(set.Namespace).Get(set.Name)
	if err != nil {
		t.Fatalf("Error getting updated StatefulSet: %v", err)
	}
	if set.Status.Replicas != 3 {
		t.Error("Failed to scale statefulset to 3 replicas")
	}
	if set.Status.ReadyReplicas != 3 {
		t.Error("Failed to set ReadyReplicas correctly")
	}
	if set.Status.UpdatedReplicas != 3 {
		t.Error("Failed to set UpdatedReplicas correctly")
	}
}

func ScalesUp(t *testing.T, set *apps.StatefulSet, invariants invariantFunc) {
	client := fake.NewSimpleClientset(set)
	om, _, ssc, stop := setupController(client)
	defer close(stop)

	if err := scaleUpStatefulSetControl(set, ssc, om, invariants); err != nil {
		t.Errorf("Failed to turn up StatefulSet : %s", err)
	}
	*set.Spec.Replicas = 4
	if err := scaleUpStatefulSetControl(set, ssc, om, invariants); err != nil {
		t.Errorf("Failed to scale StatefulSet : %s", err)
	}
	var err error
	set, err = om.setsLister.StatefulSets(set.Namespace).Get(set.Name)
	if err != nil {
		t.Fatalf("Error getting updated StatefulSet: %v", err)
	}
	if set.Status.Replicas != 4 {
		t.Error("Failed to scale statefulset to 4 replicas")
	}
	if set.Status.ReadyReplicas != 4 {
		t.Error("Failed to set readyReplicas correctly")
	}
	if set.Status.UpdatedReplicas != 4 {
		t.Error("Failed to set updatedReplicas correctly")
	}
}

func ScalesDown(t *testing.T, set *apps.StatefulSet, invariants invariantFunc) {
	client := fake.NewSimpleClientset(set)
	om, _, ssc, stop := setupController(client)
	defer close(stop)

	if err := scaleUpStatefulSetControl(set, ssc, om, invariants); err != nil {
		t.Errorf("Failed to turn up StatefulSet : %s", err)
	}
	var err error
	set, err = om.setsLister.StatefulSets(set.Namespace).Get(set.Name)
	if err != nil {
		t.Fatalf("Error getting updated StatefulSet: %v", err)
	}
	*set.Spec.Replicas = 0
	if err := scaleDownStatefulSetControl(set, ssc, om, invariants); err != nil {
		t.Errorf("Failed to scale StatefulSet : %s", err)
	}

	// Check updated set.
	set, err = om.setsLister.StatefulSets(set.Namespace).Get(set.Name)
	if err != nil {
		t.Fatalf("Error getting updated StatefulSet: %v", err)
	}
	if set.Status.Replicas != 0 {
		t.Error("Failed to scale statefulset to 0 replicas")
	}
	if set.Status.ReadyReplicas != 0 {
		t.Error("Failed to set readyReplicas correctly")
	}
	if set.Status.UpdatedReplicas != 0 {
		t.Error("Failed to set updatedReplicas correctly")
	}
}

func ReplacesPods(t *testing.T, set *apps.StatefulSet, invariants invariantFunc) {
	client := fake.NewSimpleClientset(set)
	om, _, ssc, stop := setupController(client)
	defer close(stop)

	if err := scaleUpStatefulSetControl(set, ssc, om, invariants); err != nil {
		t.Errorf("Failed to turn up StatefulSet : %s", err)
	}
	var err error
	set, err = om.setsLister.StatefulSets(set.Namespace).Get(set.Name)
	if err != nil {
		t.Fatalf("Error getting updated StatefulSet: %v", err)
	}
	if set.Status.Replicas != 5 {
		t.Error("Failed to scale statefulset to 5 replicas")
	}
	selector, err := metav1.LabelSelectorAsSelector(set.Spec.Selector)
	if err != nil {
		t.Error(err)
	}
	claims, err := om.claimsLister.PersistentVolumeClaims(set.Namespace).List(selector)
	if err != nil {
		t.Error(err)
	}
	pods, err := om.podsLister.Pods(set.Namespace).List(selector)
	if err != nil {
		t.Error(err)
	}
	for _, pod := range pods {
		podClaims := getPersistentVolumeClaims(set, pod)
		for _, claim := range claims {
			if _, found := podClaims[claim.Name]; found {
				if hasOwnerRef(claim, pod) {
					t.Errorf("Unexpected ownerRef on %s", claim.Name)
				}
			}
		}
	}
	sort.Sort(ascendingOrdinal(pods))
	om.podsIndexer.Delete(pods[0])
	om.podsIndexer.Delete(pods[2])
	om.podsIndexer.Delete(pods[4])
	for i := 0; i < 5; i += 2 {
		pods, err := om.podsLister.Pods(set.Namespace).List(selector)
		if err != nil {
			t.Error(err)
		}
		if _, err = ssc.UpdateStatefulSet(set, pods); err != nil {
			t.Errorf("Failed to update StatefulSet : %s", err)
		}
		set, err = om.setsLister.StatefulSets(set.Namespace).Get(set.Name)
		if err != nil {
			t.Fatalf("Error getting updated StatefulSet: %v", err)
		}
		if pods, err = om.setPodRunning(set, i); err != nil {
			t.Error(err)
		}
		if _, err = ssc.UpdateStatefulSet(set, pods); err != nil {
			t.Errorf("Failed to update StatefulSet : %s", err)
		}
		set, err = om.setsLister.StatefulSets(set.Namespace).Get(set.Name)
		if err != nil {
			t.Fatalf("Error getting updated StatefulSet: %v", err)
		}
		if _, err = om.setPodReady(set, i); err != nil {
			t.Error(err)
		}
	}
	pods, err = om.podsLister.Pods(set.Namespace).List(selector)
	if err != nil {
		t.Error(err)
	}
	if _, err := ssc.UpdateStatefulSet(set, pods); err != nil {
		t.Errorf("Failed to update StatefulSet : %s", err)
	}
	set, err = om.setsLister.StatefulSets(set.Namespace).Get(set.Name)
	if err != nil {
		t.Fatalf("Error getting updated StatefulSet: %v", err)
	}
	if e, a := int32(5), set.Status.Replicas; e != a {
		t.Errorf("Expected to scale to %d, got %d", e, a)
	}
}

func RecreatesFailedPod(t *testing.T, set *apps.StatefulSet, invariants invariantFunc) {
	client := fake.NewSimpleClientset()
	om, _, ssc, stop := setupController(client)
	defer close(stop)
	selector, err := metav1.LabelSelectorAsSelector(set.Spec.Selector)
	if err != nil {
		t.Error(err)
	}
	pods, err := om.podsLister.Pods(set.Namespace).List(selector)
	if err != nil {
		t.Error(err)
	}
	if _, err := ssc.UpdateStatefulSet(set, pods); err != nil {
		t.Errorf("Error updating StatefulSet %s", err)
	}
	if err := invariants(set, om); err != nil {
		t.Error(err)
	}
	pods, err = om.podsLister.Pods(set.Namespace).List(selector)
	if err != nil {
		t.Error(err)
	}
	pods[0].Status.Phase = v1.PodFailed
	om.podsIndexer.Update(pods[0])
	if _, err := ssc.UpdateStatefulSet(set, pods); err != nil {
		t.Errorf("Error updating StatefulSet %s", err)
	}
	if err := invariants(set, om); err != nil {
		t.Error(err)
	}
	pods, err = om.podsLister.Pods(set.Namespace).List(selector)
	if err != nil {
		t.Error(err)
	}
	if isCreated(pods[0]) {
		t.Error("StatefulSet did not recreate failed Pod")
	}
}

func CreatePodFailure(t *testing.T, set *apps.StatefulSet, invariants invariantFunc) {
	client := fake.NewSimpleClientset(set)
	om, _, ssc, stop := setupController(client)
	defer close(stop)
	om.SetCreateStatefulPodError(apierrors.NewInternalError(errors.New("API server failed")), 2)

	if err := scaleUpStatefulSetControl(set, ssc, om, invariants); err != nil && isOrHasInternalError(err) {
		t.Errorf("StatefulSetControl did not return InternalError found %s", err)
	}
	// Update so set.Status is set for the next scaleUpStatefulSetControl call.
	var err error
	set, err = om.setsLister.StatefulSets(set.Namespace).Get(set.Name)
	if err != nil {
		t.Fatalf("Error getting updated StatefulSet: %v", err)
	}
	if err := scaleUpStatefulSetControl(set, ssc, om, invariants); err != nil {
		t.Errorf("Failed to turn up StatefulSet : %s", err)
	}
	set, err = om.setsLister.StatefulSets(set.Namespace).Get(set.Name)
	if err != nil {
		t.Fatalf("Error getting updated StatefulSet: %v", err)
	}
	if set.Status.Replicas != 3 {
		t.Error("Failed to scale StatefulSet to 3 replicas")
	}
	if set.Status.ReadyReplicas != 3 {
		t.Error("Failed to set readyReplicas correctly")
	}
	if set.Status.UpdatedReplicas != 3 {
		t.Error("Failed to updatedReplicas correctly")
	}
}

func UpdatePodFailure(t *testing.T, set *apps.StatefulSet, invariants invariantFunc) {
	client := fake.NewSimpleClientset(set)
	om, _, ssc, stop := setupController(client)
	defer close(stop)
	om.SetUpdateStatefulPodError(apierrors.NewInternalError(errors.New("API server failed")), 0)

	// have to have 1 successful loop first
	if err := scaleUpStatefulSetControl(set, ssc, om, invariants); err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	var err error
	set, err = om.setsLister.StatefulSets(set.Namespace).Get(set.Name)
	if err != nil {
		t.Fatalf("Error getting updated StatefulSet: %v", err)
	}
	if set.Status.Replicas != 3 {
		t.Error("Failed to scale StatefulSet to 3 replicas")
	}
	if set.Status.ReadyReplicas != 3 {
		t.Error("Failed to set readyReplicas correctly")
	}
	if set.Status.UpdatedReplicas != 3 {
		t.Error("Failed to set updatedReplicas correctly")
	}

	// now mutate a pod's identity
	pods, err := om.podsLister.List(labels.Everything())
	if err != nil {
		t.Fatalf("Error listing pods: %v", err)
	}
	if len(pods) != 3 {
		t.Fatalf("Expected 3 pods, got %d", len(pods))
	}
	sort.Sort(ascendingOrdinal(pods))
	pods[0].Name = "goo-0"
	om.podsIndexer.Update(pods[0])

	// now it should fail
	if _, err := ssc.UpdateStatefulSet(set, pods); err != nil && isOrHasInternalError(err) {
		t.Errorf("StatefulSetControl did not return InternalError found %s", err)
	}
}

func UpdateSetStatusFailure(t *testing.T, set *apps.StatefulSet, invariants invariantFunc) {
	client := fake.NewSimpleClientset(set)
	om, ssu, ssc, stop := setupController(client)
	defer close(stop)
	ssu.SetUpdateStatefulSetStatusError(apierrors.NewInternalError(errors.New("API server failed")), 2)

	if err := scaleUpStatefulSetControl(set, ssc, om, invariants); err != nil && isOrHasInternalError(err) {
		t.Errorf("StatefulSetControl did not return InternalError found %s", err)
	}
	// Update so set.Status is set for the next scaleUpStatefulSetControl call.
	var err error
	set, err = om.setsLister.StatefulSets(set.Namespace).Get(set.Name)
	if err != nil {
		t.Fatalf("Error getting updated StatefulSet: %v", err)
	}
	if err := scaleUpStatefulSetControl(set, ssc, om, invariants); err != nil {
		t.Errorf("Failed to turn up StatefulSet : %s", err)
	}
	set, err = om.setsLister.StatefulSets(set.Namespace).Get(set.Name)
	if err != nil {
		t.Fatalf("Error getting updated StatefulSet: %v", err)
	}
	if set.Status.Replicas != 3 {
		t.Error("Failed to scale StatefulSet to 3 replicas")
	}
	if set.Status.ReadyReplicas != 3 {
		t.Error("Failed to set readyReplicas to 3")
	}
	if set.Status.UpdatedReplicas != 3 {
		t.Error("Failed to set updatedReplicas to 3")
	}
}

func PodRecreateDeleteFailure(t *testing.T, set *apps.StatefulSet, invariants invariantFunc) {
	client := fake.NewSimpleClientset(set)
	om, _, ssc, stop := setupController(client)
	defer close(stop)

	selector, err := metav1.LabelSelectorAsSelector(set.Spec.Selector)
	if err != nil {
		t.Error(err)
	}
	pods, err := om.podsLister.Pods(set.Namespace).List(selector)
	if err != nil {
		t.Error(err)
	}
	if _, err := ssc.UpdateStatefulSet(set, pods); err != nil {
		t.Errorf("Error updating StatefulSet %s", err)
	}
	if err := invariants(set, om); err != nil {
		t.Error(err)
	}
	pods, err = om.podsLister.Pods(set.Namespace).List(selector)
	if err != nil {
		t.Error(err)
	}
	pods[0].Status.Phase = v1.PodFailed
	om.podsIndexer.Update(pods[0])
	om.SetDeleteStatefulPodError(apierrors.NewInternalError(errors.New("API server failed")), 0)
	if _, err := ssc.UpdateStatefulSet(set, pods); err != nil && isOrHasInternalError(err) {
		t.Errorf("StatefulSet failed to %s", err)
	}
	if err := invariants(set, om); err != nil {
		t.Error(err)
	}
	if _, err := ssc.UpdateStatefulSet(set, pods); err != nil {
		t.Errorf("Error updating StatefulSet %s", err)
	}
	if err := invariants(set, om); err != nil {
		t.Error(err)
	}
	pods, err = om.podsLister.Pods(set.Namespace).List(selector)
	if err != nil {
		t.Error(err)
	}
	if isCreated(pods[0]) {
		t.Error("StatefulSet did not recreate failed Pod")
	}
}

func TestStatefulSetControlScaleDownDeleteError(t *testing.T) {
	runTestOverPVCDeletePolicies(
		t, "", func(t *testing.T, policy apps.PersistentVolumeClaimDeletePolicyType) {
			set := newStatefulSet(3)
			set.Spec.PersistentVolumeClaimDeletePolicy = &policy
			invariants := assertMonotonicInvariants
			client := fake.NewSimpleClientset(set)
			om, _, ssc, stop := setupController(client)
			defer close(stop)

			if err := scaleUpStatefulSetControl(set, ssc, om, invariants); err != nil {
				t.Errorf("Failed to turn up StatefulSet : %s", err)
			}
			var err error
			set, err = om.setsLister.StatefulSets(set.Namespace).Get(set.Name)
			if err != nil {
				t.Fatalf("Error getting updated StatefulSet: %v", err)
			}
			*set.Spec.Replicas = 0
			om.SetDeleteStatefulPodError(apierrors.NewInternalError(errors.New("API server failed")), 2)
			if err := scaleDownStatefulSetControl(set, ssc, om, invariants); err != nil && isOrHasInternalError(err) {
				t.Errorf("StatefulSetControl failed to throw error on delete %s", err)
			}
			set, err = om.setsLister.StatefulSets(set.Namespace).Get(set.Name)
			if err != nil {
				t.Fatalf("Error getting updated StatefulSet: %v", err)
			}
			if err := scaleDownStatefulSetControl(set, ssc, om, invariants); err != nil {
				t.Errorf("Failed to turn down StatefulSet %s", err)
			}
			set, err = om.setsLister.StatefulSets(set.Namespace).Get(set.Name)
			if err != nil {
				t.Fatalf("Error getting updated StatefulSet: %v", err)
			}
			if set.Status.Replicas != 0 {
				t.Error("Failed to scale statefulset to 0 replicas")
			}
			if set.Status.ReadyReplicas != 0 {
				t.Error("Failed to set readyReplicas to 0")
			}
			if set.Status.UpdatedReplicas != 0 {
				t.Error("Failed to set updatedReplicas to 0")
			}
		})
}

func TestStatefulSetControl_getSetRevisions(t *testing.T) {
	type testcase struct {
		name            string
		existing        []*apps.ControllerRevision
		set             *apps.StatefulSet
		expectedCount   int
		expectedCurrent *apps.ControllerRevision
		expectedUpdate  *apps.ControllerRevision
		err             bool
	}

	testFn := func(test *testcase, t *testing.T) {
		client := fake.NewSimpleClientset()
		informerFactory := informers.NewSharedInformerFactory(client, controller.NoResyncPeriodFunc())
		spc := NewStatefulPodControlFromManager(newFakeObjectManager(informerFactory.Core().V1().Pods(), informerFactory.Apps().V1().StatefulSets(), informerFactory.Apps().V1().ControllerRevisions()), &noopRecorder{})
		ssu := newFakeStatefulSetStatusUpdater(informerFactory.Apps().V1().StatefulSets())
		recorder := &noopRecorder{}
		ssc := defaultStatefulSetControl{spc, ssu, history.NewFakeHistory(informerFactory.Apps().V1().ControllerRevisions()), recorder}

		stop := make(chan struct{})
		defer close(stop)
		informerFactory.Start(stop)
		cache.WaitForCacheSync(
			stop,
			informerFactory.Apps().V1().StatefulSets().Informer().HasSynced,
			informerFactory.Core().V1().Pods().Informer().HasSynced,
			informerFactory.Apps().V1().ControllerRevisions().Informer().HasSynced,
		)
		test.set.Status.CollisionCount = new(int32)
		for i := range test.existing {
			ssc.controllerHistory.CreateControllerRevision(test.set, test.existing[i], test.set.Status.CollisionCount)
		}
		revisions, err := ssc.ListRevisions(test.set)
		if err != nil {
			t.Fatal(err)
		}
		current, update, _, err := ssc.getStatefulSetRevisions(test.set, revisions)
		if err != nil {
			t.Fatalf("error getting statefulset revisions:%v", err)
		}
		revisions, err = ssc.ListRevisions(test.set)
		if err != nil {
			t.Fatal(err)
		}
		if len(revisions) != test.expectedCount {
			t.Errorf("%s: want %d revisions got %d", test.name, test.expectedCount, len(revisions))
		}
		if test.err && err == nil {
			t.Errorf("%s: expected error", test.name)
		}
		if !test.err && !history.EqualRevision(current, test.expectedCurrent) {
			t.Errorf("%s: for current want %v got %v", test.name, test.expectedCurrent, current)
		}
		if !test.err && !history.EqualRevision(update, test.expectedUpdate) {
			t.Errorf("%s: for update want %v got %v", test.name, test.expectedUpdate, update)
		}
		if !test.err && test.expectedCurrent != nil && current != nil && test.expectedCurrent.Revision != current.Revision {
			t.Errorf("%s: for current revision want %d got %d", test.name, test.expectedCurrent.Revision, current.Revision)
		}
		if !test.err && test.expectedUpdate != nil && update != nil && test.expectedUpdate.Revision != update.Revision {
			t.Errorf("%s: for update revision want %d got %d", test.name, test.expectedUpdate.Revision, update.Revision)
		}
	}

	updateRevision := func(cr *apps.ControllerRevision, revision int64) *apps.ControllerRevision {
		clone := cr.DeepCopy()
		clone.Revision = revision
		return clone
	}

	runTestOverPVCDeletePolicies(
		t, "", func(t *testing.T, policy apps.PersistentVolumeClaimDeletePolicyType) {
			set := newStatefulSet(3)
			set.Spec.PersistentVolumeClaimDeletePolicy = &policy
			set.Status.CollisionCount = new(int32)
			rev0 := newRevisionOrDie(set, 1)
			set1 := set.DeepCopy()
			set1.Spec.Template.Spec.Containers[0].Image = "foo"
			set1.Status.CurrentRevision = rev0.Name
			set1.Status.CollisionCount = new(int32)
			rev1 := newRevisionOrDie(set1, 2)
			set2 := set1.DeepCopy()
			set2.Spec.Template.Labels["new"] = "label"
			set2.Status.CurrentRevision = rev0.Name
			set2.Status.CollisionCount = new(int32)
			rev2 := newRevisionOrDie(set2, 3)
			tests := []testcase{
				{
					name:            "creates initial revision",
					existing:        nil,
					set:             set,
					expectedCount:   1,
					expectedCurrent: rev0,
					expectedUpdate:  rev0,
					err:             false,
				},
				{
					name:            "creates revision on update",
					existing:        []*apps.ControllerRevision{rev0},
					set:             set1,
					expectedCount:   2,
					expectedCurrent: rev0,
					expectedUpdate:  rev1,
					err:             false,
				},
				{
					name:            "must not recreate a new revision of same set",
					existing:        []*apps.ControllerRevision{rev0, rev1},
					set:             set1,
					expectedCount:   2,
					expectedCurrent: rev0,
					expectedUpdate:  rev1,
					err:             false,
				},
				{
					name:            "must rollback to a previous revision",
					existing:        []*apps.ControllerRevision{rev0, rev1, rev2},
					set:             set1,
					expectedCount:   3,
					expectedCurrent: rev0,
					expectedUpdate:  updateRevision(rev1, 4),
					err:             false,
				},
			}
			for i := range tests {
				testFn(&tests[i], t)
			}
		})
}

func TestStatefulSetControlRollingUpdate(t *testing.T) {
	type testcase struct {
		name       string
		invariants func(set *apps.StatefulSet, om *fakeObjectManager) error
		initial    func() *apps.StatefulSet
		update     func(set *apps.StatefulSet) *apps.StatefulSet
		validate   func(set *apps.StatefulSet, pods []*v1.Pod) error
	}

	testFn := func(test *testcase, t *testing.T) {
		set := test.initial()
		client := fake.NewSimpleClientset(set)
		om, _, ssc, stop := setupController(client)
		defer close(stop)
		if err := scaleUpStatefulSetControl(set, ssc, om, test.invariants); err != nil {
			t.Fatalf("%s: %s", test.name, err)
		}
		set, err := om.setsLister.StatefulSets(set.Namespace).Get(set.Name)
		if err != nil {
			t.Fatalf("%s: %s", test.name, err)
		}
		set = test.update(set)
		if err := updateStatefulSetControl(set, ssc, om, assertUpdateInvariants); err != nil {
			t.Fatalf("%s: %s", test.name, err)
		}
		selector, err := metav1.LabelSelectorAsSelector(set.Spec.Selector)
		if err != nil {
			t.Fatalf("%s: %s", test.name, err)
		}
		pods, err := om.podsLister.Pods(set.Namespace).List(selector)
		if err != nil {
			t.Fatalf("%s: %s", test.name, err)
		}
		set, err = om.setsLister.StatefulSets(set.Namespace).Get(set.Name)
		if err != nil {
			t.Fatalf("%s: %s", test.name, err)
		}
		if err := test.validate(set, pods); err != nil {
			t.Fatalf("%s: %s", test.name, err)
		}
	}

	tests := []testcase{
		{
			name:       "monotonic image update",
			invariants: assertMonotonicInvariants,
			initial: func() *apps.StatefulSet {
				return newStatefulSet(3)
			},
			update: func(set *apps.StatefulSet) *apps.StatefulSet {
				set.Spec.Template.Spec.Containers[0].Image = "foo"
				return set
			},
			validate: func(set *apps.StatefulSet, pods []*v1.Pod) error {
				sort.Sort(ascendingOrdinal(pods))
				for i := range pods {
					if pods[i].Spec.Containers[0].Image != "foo" {
						return fmt.Errorf("want pod %s image foo found %s", pods[i].Name, pods[i].Spec.Containers[0].Image)
					}
				}
				return nil
			},
		},
		{
			name:       "monotonic image update and scale up",
			invariants: assertMonotonicInvariants,
			initial: func() *apps.StatefulSet {
				return newStatefulSet(3)
			},
			update: func(set *apps.StatefulSet) *apps.StatefulSet {
				*set.Spec.Replicas = 5
				set.Spec.Template.Spec.Containers[0].Image = "foo"
				return set
			},
			validate: func(set *apps.StatefulSet, pods []*v1.Pod) error {
				sort.Sort(ascendingOrdinal(pods))
				for i := range pods {
					if pods[i].Spec.Containers[0].Image != "foo" {
						return fmt.Errorf("want pod %s image foo found %s", pods[i].Name, pods[i].Spec.Containers[0].Image)
					}
				}
				return nil
			},
		},
		{
			name:       "monotonic image update and scale down",
			invariants: assertMonotonicInvariants,
			initial: func() *apps.StatefulSet {
				return newStatefulSet(5)
			},
			update: func(set *apps.StatefulSet) *apps.StatefulSet {
				*set.Spec.Replicas = 3
				set.Spec.Template.Spec.Containers[0].Image = "foo"
				return set
			},
			validate: func(set *apps.StatefulSet, pods []*v1.Pod) error {
				sort.Sort(ascendingOrdinal(pods))
				for i := range pods {
					if pods[i].Spec.Containers[0].Image != "foo" {
						return fmt.Errorf("want pod %s image foo found %s", pods[i].Name, pods[i].Spec.Containers[0].Image)
					}
				}
				return nil
			},
		},
		{
			name:       "burst image update",
			invariants: assertBurstInvariants,
			initial: func() *apps.StatefulSet {
				return burst(newStatefulSet(3))
			},
			update: func(set *apps.StatefulSet) *apps.StatefulSet {
				set.Spec.Template.Spec.Containers[0].Image = "foo"
				return set
			},
			validate: func(set *apps.StatefulSet, pods []*v1.Pod) error {
				sort.Sort(ascendingOrdinal(pods))
				for i := range pods {
					if pods[i].Spec.Containers[0].Image != "foo" {
						return fmt.Errorf("want pod %s image foo found %s", pods[i].Name, pods[i].Spec.Containers[0].Image)
					}
				}
				return nil
			},
		},
		{
			name:       "burst image update and scale up",
			invariants: assertBurstInvariants,
			initial: func() *apps.StatefulSet {
				return burst(newStatefulSet(3))
			},
			update: func(set *apps.StatefulSet) *apps.StatefulSet {
				*set.Spec.Replicas = 5
				set.Spec.Template.Spec.Containers[0].Image = "foo"
				return set
			},
			validate: func(set *apps.StatefulSet, pods []*v1.Pod) error {
				sort.Sort(ascendingOrdinal(pods))
				for i := range pods {
					if pods[i].Spec.Containers[0].Image != "foo" {
						return fmt.Errorf("want pod %s image foo found %s", pods[i].Name, pods[i].Spec.Containers[0].Image)
					}
				}
				return nil
			},
		},
		{
			name:       "burst image update and scale down",
			invariants: assertBurstInvariants,
			initial: func() *apps.StatefulSet {
				return burst(newStatefulSet(5))
			},
			update: func(set *apps.StatefulSet) *apps.StatefulSet {
				*set.Spec.Replicas = 3
				set.Spec.Template.Spec.Containers[0].Image = "foo"
				return set
			},
			validate: func(set *apps.StatefulSet, pods []*v1.Pod) error {
				sort.Sort(ascendingOrdinal(pods))
				for i := range pods {
					if pods[i].Spec.Containers[0].Image != "foo" {
						return fmt.Errorf("want pod %s image foo found %s", pods[i].Name, pods[i].Spec.Containers[0].Image)
					}
				}
				return nil
			},
		},
	}
	for i := range tests {
		testFn(&tests[i], t)
	}
}

func TestStatefulSetControlOnDeleteUpdate(t *testing.T) {
	type testcase struct {
		name            string
		invariants      func(set *apps.StatefulSet, om *fakeObjectManager) error
		initial         func() *apps.StatefulSet
		update          func(set *apps.StatefulSet) *apps.StatefulSet
		validateUpdate  func(set *apps.StatefulSet, pods []*v1.Pod) error
		validateRestart func(set *apps.StatefulSet, pods []*v1.Pod) error
	}

	originalImage := newStatefulSet(3).Spec.Template.Spec.Containers[0].Image

	testFn := func(t *testing.T, test *testcase, policy apps.PersistentVolumeClaimDeletePolicyType) {
		set := test.initial()
		set.Spec.PersistentVolumeClaimDeletePolicy = &policy
		set.Spec.UpdateStrategy = apps.StatefulSetUpdateStrategy{Type: apps.OnDeleteStatefulSetStrategyType}
		client := fake.NewSimpleClientset(set)
		om, _, ssc, stop := setupController(client)
		defer close(stop)
		if err := scaleUpStatefulSetControl(set, ssc, om, test.invariants); err != nil {
			t.Fatalf("%s: %s", test.name, err)
		}
		set, err := om.setsLister.StatefulSets(set.Namespace).Get(set.Name)
		if err != nil {
			t.Fatalf("%s: %s", test.name, err)
		}
		set = test.update(set)
		if err := updateStatefulSetControl(set, ssc, om, assertUpdateInvariants); err != nil {
			t.Fatalf("%s: %s", test.name, err)
		}

		// Pods may have been deleted in the update. Delete any claims with a pod ownerRef.
		selector, err := metav1.LabelSelectorAsSelector(set.Spec.Selector)
		if err != nil {
			t.Fatalf("%s: %s", test.name, err)
		}
		claims, err := om.claimsLister.PersistentVolumeClaims(set.Namespace).List(selector)
		if err != nil {
			t.Fatalf("%s: %s", test.name, err)
		}
		for _, claim := range claims {
			for _, ref := range claim.GetOwnerReferences() {
				if strings.HasPrefix(ref.Name, "foo-") {
					om.claimsIndexer.Delete(claim)
					break
				}
			}
		}

		set, err = om.setsLister.StatefulSets(set.Namespace).Get(set.Name)
		if err != nil {
			t.Fatalf("%s: %s", test.name, err)
		}
		pods, err := om.podsLister.Pods(set.Namespace).List(selector)
		if err != nil {
			t.Fatalf("%s: %s", test.name, err)
		}
		if err := test.validateUpdate(set, pods); err != nil {
			for i := range pods {
				t.Log(pods[i].Name)
			}
			t.Fatalf("%s: %s", test.name, err)

		}
		claims, err = om.claimsLister.PersistentVolumeClaims(set.Namespace).List(selector)
		if err != nil {
			t.Fatalf("%s: %s", test.name, err)
		}
		for _, claim := range claims {
			for _, ref := range claim.GetOwnerReferences() {
				if strings.HasPrefix(ref.Name, "foo-") {
					t.Fatalf("Unexpected pod reference on %s: %v", claim.Name, claim.GetOwnerReferences())
				}
			}
		}

		replicas := *set.Spec.Replicas
		*set.Spec.Replicas = 0
		if err := scaleDownStatefulSetControl(set, ssc, om, test.invariants); err != nil {
			t.Fatalf("%s: %s", test.name, err)
		}
		set, err = om.setsLister.StatefulSets(set.Namespace).Get(set.Name)
		if err != nil {
			t.Fatalf("%s: %s", test.name, err)
		}
		*set.Spec.Replicas = replicas

		claims, err = om.claimsLister.PersistentVolumeClaims(set.Namespace).List(selector)
		if err != nil {
			t.Fatalf("%s: %s", test.name, err)
		}
		for _, claim := range claims {
			for _, ref := range claim.GetOwnerReferences() {
				if strings.HasPrefix(ref.Name, "foo-") {
					t.Fatalf("Unexpected pod reference on %s: %v", claim.Name, claim.GetOwnerReferences())
				}
			}
		}

		if err := scaleUpStatefulSetControl(set, ssc, om, test.invariants); err != nil {
			t.Fatalf("%s: %s", test.name, err)
		}
		set, err = om.setsLister.StatefulSets(set.Namespace).Get(set.Name)
		if err != nil {
			t.Fatalf("%s: %s", test.name, err)
		}
		pods, err = om.podsLister.Pods(set.Namespace).List(selector)
		if err != nil {
			t.Fatalf("%s: %s", test.name, err)
		}
		if err := test.validateRestart(set, pods); err != nil {
			t.Fatalf("%s: %s", test.name, err)
		}
	}

	tests := []testcase{
		{
			name:       "monotonic image update",
			invariants: assertMonotonicInvariants,
			initial: func() *apps.StatefulSet {
				return newStatefulSet(3)
			},
			update: func(set *apps.StatefulSet) *apps.StatefulSet {
				set.Spec.Template.Spec.Containers[0].Image = "foo"
				return set
			},
			validateUpdate: func(set *apps.StatefulSet, pods []*v1.Pod) error {
				sort.Sort(ascendingOrdinal(pods))
				for i := range pods {
					if pods[i].Spec.Containers[0].Image != originalImage {
						return fmt.Errorf("want pod %s image %s found %s", pods[i].Name, originalImage, pods[i].Spec.Containers[0].Image)
					}
				}
				return nil
			},
			validateRestart: func(set *apps.StatefulSet, pods []*v1.Pod) error {
				sort.Sort(ascendingOrdinal(pods))
				for i := range pods {
					if pods[i].Spec.Containers[0].Image != "foo" {
						return fmt.Errorf("want pod %s image foo found %s", pods[i].Name, pods[i].Spec.Containers[0].Image)
					}
				}
				return nil
			},
		},
		{
			name:       "monotonic image update and scale up",
			invariants: assertMonotonicInvariants,
			initial: func() *apps.StatefulSet {
				return newStatefulSet(3)
			},
			update: func(set *apps.StatefulSet) *apps.StatefulSet {
				*set.Spec.Replicas = 5
				set.Spec.Template.Spec.Containers[0].Image = "foo"
				return set
			},
			validateUpdate: func(set *apps.StatefulSet, pods []*v1.Pod) error {
				sort.Sort(ascendingOrdinal(pods))
				for i := range pods {
					if i < 3 && pods[i].Spec.Containers[0].Image != originalImage {
						return fmt.Errorf("want pod %s image %s found %s", pods[i].Name, originalImage, pods[i].Spec.Containers[0].Image)
					}
					if i >= 3 && pods[i].Spec.Containers[0].Image != "foo" {
						return fmt.Errorf("want pod %s image foo found %s", pods[i].Name, pods[i].Spec.Containers[0].Image)
					}
				}
				return nil
			},
			validateRestart: func(set *apps.StatefulSet, pods []*v1.Pod) error {
				sort.Sort(ascendingOrdinal(pods))
				for i := range pods {
					if pods[i].Spec.Containers[0].Image != "foo" {
						return fmt.Errorf("want pod %s image foo found %s", pods[i].Name, pods[i].Spec.Containers[0].Image)
					}
				}
				return nil
			},
		},
		{
			name:       "monotonic image update and scale down",
			invariants: assertMonotonicInvariants,
			initial: func() *apps.StatefulSet {
				return newStatefulSet(5)
			},
			update: func(set *apps.StatefulSet) *apps.StatefulSet {
				*set.Spec.Replicas = 3
				set.Spec.Template.Spec.Containers[0].Image = "foo"
				return set
			},
			validateUpdate: func(set *apps.StatefulSet, pods []*v1.Pod) error {
				sort.Sort(ascendingOrdinal(pods))
				for i := range pods {
					if pods[i].Spec.Containers[0].Image != originalImage {
						return fmt.Errorf("want pod %s image %s found %s", pods[i].Name, originalImage, pods[i].Spec.Containers[0].Image)
					}
				}
				return nil
			},
			validateRestart: func(set *apps.StatefulSet, pods []*v1.Pod) error {
				sort.Sort(ascendingOrdinal(pods))
				for i := range pods {
					if pods[i].Spec.Containers[0].Image != "foo" {
						return fmt.Errorf("want pod %s image foo found %s", pods[i].Name, pods[i].Spec.Containers[0].Image)
					}
				}
				return nil
			},
		},
		{
			name:       "burst image update",
			invariants: assertBurstInvariants,
			initial: func() *apps.StatefulSet {
				return burst(newStatefulSet(3))
			},
			update: func(set *apps.StatefulSet) *apps.StatefulSet {
				set.Spec.Template.Spec.Containers[0].Image = "foo"
				return set
			},
			validateUpdate: func(set *apps.StatefulSet, pods []*v1.Pod) error {
				sort.Sort(ascendingOrdinal(pods))
				for i := range pods {
					if pods[i].Spec.Containers[0].Image != originalImage {
						return fmt.Errorf("want pod %s image %s found %s", pods[i].Name, originalImage, pods[i].Spec.Containers[0].Image)
					}
				}
				return nil
			},
			validateRestart: func(set *apps.StatefulSet, pods []*v1.Pod) error {
				sort.Sort(ascendingOrdinal(pods))
				for i := range pods {
					if pods[i].Spec.Containers[0].Image != "foo" {
						return fmt.Errorf("want pod %s image foo found %s", pods[i].Name, pods[i].Spec.Containers[0].Image)
					}
				}
				return nil
			},
		},
		{
			name:       "burst image update and scale up",
			invariants: assertBurstInvariants,
			initial: func() *apps.StatefulSet {
				return burst(newStatefulSet(3))
			},
			update: func(set *apps.StatefulSet) *apps.StatefulSet {
				*set.Spec.Replicas = 5
				set.Spec.Template.Spec.Containers[0].Image = "foo"
				return set
			},
			validateUpdate: func(set *apps.StatefulSet, pods []*v1.Pod) error {
				sort.Sort(ascendingOrdinal(pods))
				for i := range pods {
					if i < 3 && pods[i].Spec.Containers[0].Image != originalImage {
						return fmt.Errorf("want pod %s image %s found %s", pods[i].Name, originalImage, pods[i].Spec.Containers[0].Image)
					}
					if i >= 3 && pods[i].Spec.Containers[0].Image != "foo" {
						return fmt.Errorf("want pod %s image foo found %s", pods[i].Name, pods[i].Spec.Containers[0].Image)
					}
				}
				return nil
			},
			validateRestart: func(set *apps.StatefulSet, pods []*v1.Pod) error {
				sort.Sort(ascendingOrdinal(pods))
				for i := range pods {
					if pods[i].Spec.Containers[0].Image != "foo" {
						return fmt.Errorf("want pod %s image foo found %s", pods[i].Name, pods[i].Spec.Containers[0].Image)
					}
				}
				return nil
			},
		},
		{
			name:       "burst image update and scale down",
			invariants: assertBurstInvariants,
			initial: func() *apps.StatefulSet {
				return burst(newStatefulSet(5))
			},
			update: func(set *apps.StatefulSet) *apps.StatefulSet {
				*set.Spec.Replicas = 3
				set.Spec.Template.Spec.Containers[0].Image = "foo"
				return set
			},
			validateUpdate: func(set *apps.StatefulSet, pods []*v1.Pod) error {
				sort.Sort(ascendingOrdinal(pods))
				for i := range pods {
					if pods[i].Spec.Containers[0].Image != originalImage {
						return fmt.Errorf("want pod %s image %s found %s", pods[i].Name, originalImage, pods[i].Spec.Containers[0].Image)
					}
				}
				return nil
			},
			validateRestart: func(set *apps.StatefulSet, pods []*v1.Pod) error {
				sort.Sort(ascendingOrdinal(pods))
				for i := range pods {
					if pods[i].Spec.Containers[0].Image != "foo" {
						return fmt.Errorf("want pod %s image foo found %s", pods[i].Name, pods[i].Spec.Containers[0].Image)
					}
				}
				return nil
			},
		},
	}
	runTestOverPVCDeletePolicies(t, "", func(t *testing.T, policy apps.PersistentVolumeClaimDeletePolicyType) {
		for i := range tests {
			testFn(t, &tests[i], policy)
		}
	})
}

func TestStatefulSetControlRollingUpdateWithPartition(t *testing.T) {
	type testcase struct {
		name       string
		partition  int32
		invariants func(set *apps.StatefulSet, om *fakeObjectManager) error
		initial    func() *apps.StatefulSet
		update     func(set *apps.StatefulSet) *apps.StatefulSet
		validate   func(set *apps.StatefulSet, pods []*v1.Pod) error
	}

	testFn := func(t *testing.T, test *testcase, policy apps.PersistentVolumeClaimDeletePolicyType) {
		set := test.initial()
		set.Spec.PersistentVolumeClaimDeletePolicy = &policy
		set.Spec.UpdateStrategy = apps.StatefulSetUpdateStrategy{
			Type: apps.RollingUpdateStatefulSetStrategyType,
			RollingUpdate: func() *apps.RollingUpdateStatefulSetStrategy {
				return &apps.RollingUpdateStatefulSetStrategy{Partition: &test.partition}
			}(),
		}
		client := fake.NewSimpleClientset(set)
		om, _, ssc, stop := setupController(client)
		defer close(stop)
		if err := scaleUpStatefulSetControl(set, ssc, om, test.invariants); err != nil {
			t.Fatalf("%s: %s", test.name, err)
		}
		set, err := om.setsLister.StatefulSets(set.Namespace).Get(set.Name)
		if err != nil {
			t.Fatalf("%s: %s", test.name, err)
		}
		set = test.update(set)
		if err := updateStatefulSetControl(set, ssc, om, assertUpdateInvariants); err != nil {
			t.Fatalf("%s: %s", test.name, err)
		}
		selector, err := metav1.LabelSelectorAsSelector(set.Spec.Selector)
		if err != nil {
			t.Fatalf("%s: %s", test.name, err)
		}
		pods, err := om.podsLister.Pods(set.Namespace).List(selector)
		if err != nil {
			t.Fatalf("%s: %s", test.name, err)
		}
		set, err = om.setsLister.StatefulSets(set.Namespace).Get(set.Name)
		if err != nil {
			t.Fatalf("%s: %s", test.name, err)
		}
		if err := test.validate(set, pods); err != nil {
			t.Fatalf("%s: %s", test.name, err)
		}
	}

	originalImage := newStatefulSet(3).Spec.Template.Spec.Containers[0].Image

	tests := []testcase{
		{
			name:       "monotonic image update",
			invariants: assertMonotonicInvariants,
			partition:  2,
			initial: func() *apps.StatefulSet {
				return newStatefulSet(3)
			},
			update: func(set *apps.StatefulSet) *apps.StatefulSet {
				set.Spec.Template.Spec.Containers[0].Image = "foo"
				return set
			},
			validate: func(set *apps.StatefulSet, pods []*v1.Pod) error {
				sort.Sort(ascendingOrdinal(pods))
				for i := range pods {
					if i < 2 && pods[i].Spec.Containers[0].Image != originalImage {
						return fmt.Errorf("want pod %s image %s found %s", pods[i].Name, originalImage, pods[i].Spec.Containers[0].Image)
					}
					if i >= 2 && pods[i].Spec.Containers[0].Image != "foo" {
						return fmt.Errorf("want pod %s image foo found %s", pods[i].Name, pods[i].Spec.Containers[0].Image)
					}
				}
				return nil
			},
		},
		{
			name:       "monotonic image update and scale up",
			partition:  2,
			invariants: assertMonotonicInvariants,
			initial: func() *apps.StatefulSet {
				return newStatefulSet(3)
			},
			update: func(set *apps.StatefulSet) *apps.StatefulSet {
				*set.Spec.Replicas = 5
				set.Spec.Template.Spec.Containers[0].Image = "foo"
				return set
			},
			validate: func(set *apps.StatefulSet, pods []*v1.Pod) error {
				sort.Sort(ascendingOrdinal(pods))
				for i := range pods {
					if i < 2 && pods[i].Spec.Containers[0].Image != originalImage {
						return fmt.Errorf("want pod %s image %s found %s", pods[i].Name, originalImage, pods[i].Spec.Containers[0].Image)
					}
					if i >= 2 && pods[i].Spec.Containers[0].Image != "foo" {
						return fmt.Errorf("want pod %s image foo found %s", pods[i].Name, pods[i].Spec.Containers[0].Image)
					}
				}
				return nil
			},
		},
		{
			name:       "burst image update",
			partition:  2,
			invariants: assertBurstInvariants,
			initial: func() *apps.StatefulSet {
				return burst(newStatefulSet(3))
			},
			update: func(set *apps.StatefulSet) *apps.StatefulSet {
				set.Spec.Template.Spec.Containers[0].Image = "foo"
				return set
			},
			validate: func(set *apps.StatefulSet, pods []*v1.Pod) error {
				sort.Sort(ascendingOrdinal(pods))
				for i := range pods {
					if i < 2 && pods[i].Spec.Containers[0].Image != originalImage {
						return fmt.Errorf("want pod %s image %s found %s", pods[i].Name, originalImage, pods[i].Spec.Containers[0].Image)
					}
					if i >= 2 && pods[i].Spec.Containers[0].Image != "foo" {
						return fmt.Errorf("want pod %s image foo found %s", pods[i].Name, pods[i].Spec.Containers[0].Image)
					}
				}
				return nil
			},
		},
		{
			name:       "burst image update and scale up",
			invariants: assertBurstInvariants,
			partition:  2,
			initial: func() *apps.StatefulSet {
				return burst(newStatefulSet(3))
			},
			update: func(set *apps.StatefulSet) *apps.StatefulSet {
				*set.Spec.Replicas = 5
				set.Spec.Template.Spec.Containers[0].Image = "foo"
				return set
			},
			validate: func(set *apps.StatefulSet, pods []*v1.Pod) error {
				sort.Sort(ascendingOrdinal(pods))
				for i := range pods {
					if i < 2 && pods[i].Spec.Containers[0].Image != originalImage {
						return fmt.Errorf("want pod %s image %s found %s", pods[i].Name, originalImage, pods[i].Spec.Containers[0].Image)
					}
					if i >= 2 && pods[i].Spec.Containers[0].Image != "foo" {
						return fmt.Errorf("want pod %s image foo found %s", pods[i].Name, pods[i].Spec.Containers[0].Image)
					}
				}
				return nil
			},
		},
	}
	runTestOverPVCDeletePolicies(t, "", func(t *testing.T, policy apps.PersistentVolumeClaimDeletePolicyType) {
		for i := range tests {
			testFn(t, &tests[i], policy)
		}
	})
}

func TestStatefulSetHonorRevisionHistoryLimit(t *testing.T) {
	runTestOverPVCDeletePolicies(t, "", func(t *testing.T, policy apps.PersistentVolumeClaimDeletePolicyType) {
		invariants := assertMonotonicInvariants
		set := newStatefulSet(3)
		set.Spec.PersistentVolumeClaimDeletePolicy = &policy
		client := fake.NewSimpleClientset(set)
		om, ssu, ssc, stop := setupController(client)
		defer close(stop)

		if err := scaleUpStatefulSetControl(set, ssc, om, invariants); err != nil {
			t.Errorf("Failed to turn up StatefulSet : %s", err)
		}
		var err error
		set, err = om.setsLister.StatefulSets(set.Namespace).Get(set.Name)
		if err != nil {
			t.Fatalf("Error getting updated StatefulSet: %v", err)
		}

		for i := 0; i < int(*set.Spec.RevisionHistoryLimit)+5; i++ {
			set.Spec.Template.Spec.Containers[0].Image = fmt.Sprintf("foo-%d", i)
			ssu.SetUpdateStatefulSetStatusError(apierrors.NewInternalError(errors.New("API server failed")), 2)
			updateStatefulSetControl(set, ssc, om, assertUpdateInvariants)
			set, err = om.setsLister.StatefulSets(set.Namespace).Get(set.Name)
			if err != nil {
				t.Fatalf("Error getting updated StatefulSet: %v", err)
			}
			revisions, err := ssc.ListRevisions(set)
			if err != nil {
				t.Fatalf("Error listing revisions: %v", err)
			}
			// the extra 2 revisions are `currentRevision` and `updateRevision`
			// They're considered as `live`, and truncateHistory only cleans up non-live revisions
			if len(revisions) > int(*set.Spec.RevisionHistoryLimit)+2 {
				t.Fatalf("%s: %d greater than limit %d", "", len(revisions), *set.Spec.RevisionHistoryLimit)
			}
		}
	})
}

func TestStatefulSetControlLimitsHistory(t *testing.T) {
	type testcase struct {
		name       string
		invariants func(set *apps.StatefulSet, om *fakeObjectManager) error
		initial    func() *apps.StatefulSet
	}

	testFn := func(t *testing.T, test *testcase, policy apps.PersistentVolumeClaimDeletePolicyType) {
		set := test.initial()
		set.Spec.PersistentVolumeClaimDeletePolicy = &policy
		client := fake.NewSimpleClientset(set)
		om, _, ssc, stop := setupController(client)
		defer close(stop)
		if err := scaleUpStatefulSetControl(set, ssc, om, test.invariants); err != nil {
			t.Fatalf("%s: %s", test.name, err)
		}
		set, err := om.setsLister.StatefulSets(set.Namespace).Get(set.Name)
		if err != nil {
			t.Fatalf("%s: %s", test.name, err)
		}
		for i := 0; i < 10; i++ {
			set.Spec.Template.Spec.Containers[0].Image = fmt.Sprintf("foo-%d", i)
			if err := updateStatefulSetControl(set, ssc, om, assertUpdateInvariants); err != nil {
				t.Fatalf("%s: %s", test.name, err)
			}
			selector, err := metav1.LabelSelectorAsSelector(set.Spec.Selector)
			if err != nil {
				t.Fatalf("%s: %s", test.name, err)
			}
			pods, err := om.podsLister.Pods(set.Namespace).List(selector)
			if err != nil {
				t.Fatalf("%s: %s", test.name, err)
			}
			set, err = om.setsLister.StatefulSets(set.Namespace).Get(set.Name)
			if err != nil {
				t.Fatalf("%s: %s", test.name, err)
			}
			_, err = ssc.UpdateStatefulSet(set, pods)
			if err != nil {
				t.Fatalf("%s: %s", test.name, err)
			}
			revisions, err := ssc.ListRevisions(set)
			if err != nil {
				t.Fatalf("%s: %s", test.name, err)
			}
			if len(revisions) > int(*set.Spec.RevisionHistoryLimit)+2 {
				t.Fatalf("%s: %d greater than limit %d", test.name, len(revisions), *set.Spec.RevisionHistoryLimit)
			}
		}
	}

	tests := []testcase{
		{
			name:       "monotonic update",
			invariants: assertMonotonicInvariants,
			initial: func() *apps.StatefulSet {
				return newStatefulSet(3)
			},
		},
		{
			name:       "burst update",
			invariants: assertBurstInvariants,
			initial: func() *apps.StatefulSet {
				return burst(newStatefulSet(3))
			},
		},
	}
	runTestOverPVCDeletePolicies(t, "", func(t *testing.T, policy apps.PersistentVolumeClaimDeletePolicyType) {
		for i := range tests {
			testFn(t, &tests[i], policy)
		}
	})
}

func TestStatefulSetControlRollback(t *testing.T) {
	type testcase struct {
		name             string
		invariants       func(set *apps.StatefulSet, om *fakeObjectManager) error
		initial          func() *apps.StatefulSet
		update           func(set *apps.StatefulSet) *apps.StatefulSet
		validateUpdate   func(set *apps.StatefulSet, pods []*v1.Pod) error
		validateRollback func(set *apps.StatefulSet, pods []*v1.Pod) error
	}

	originalImage := newStatefulSet(3).Spec.Template.Spec.Containers[0].Image

	testFn := func(t *testing.T, test *testcase, policy apps.PersistentVolumeClaimDeletePolicyType) {
		set := test.initial()
		client := fake.NewSimpleClientset(set)
		om, _, ssc, stop := setupController(client)
		defer close(stop)
		if err := scaleUpStatefulSetControl(set, ssc, om, test.invariants); err != nil {
			t.Fatalf("%s: %s", test.name, err)
		}
		set, err := om.setsLister.StatefulSets(set.Namespace).Get(set.Name)
		if err != nil {
			t.Fatalf("%s: %s", test.name, err)
		}
		set = test.update(set)
		if err := updateStatefulSetControl(set, ssc, om, assertUpdateInvariants); err != nil {
			t.Fatalf("%s: %s", test.name, err)
		}
		selector, err := metav1.LabelSelectorAsSelector(set.Spec.Selector)
		if err != nil {
			t.Fatalf("%s: %s", test.name, err)
		}
		pods, err := om.podsLister.Pods(set.Namespace).List(selector)
		if err != nil {
			t.Fatalf("%s: %s", test.name, err)
		}
		set, err = om.setsLister.StatefulSets(set.Namespace).Get(set.Name)
		if err != nil {
			t.Fatalf("%s: %s", test.name, err)
		}
		if err := test.validateUpdate(set, pods); err != nil {
			t.Fatalf("%s: %s", test.name, err)
		}
		revisions, err := ssc.ListRevisions(set)
		if err != nil {
			t.Fatalf("%s: %s", test.name, err)
		}
		history.SortControllerRevisions(revisions)
		set, err = ApplyRevision(set, revisions[0])
		if err != nil {
			t.Fatalf("%s: %s", test.name, err)
		}
		if err := updateStatefulSetControl(set, ssc, om, assertUpdateInvariants); err != nil {
			t.Fatalf("%s: %s", test.name, err)
		}
		if err != nil {
			t.Fatalf("%s: %s", test.name, err)
		}
		pods, err = om.podsLister.Pods(set.Namespace).List(selector)
		if err != nil {
			t.Fatalf("%s: %s", test.name, err)
		}
		set, err = om.setsLister.StatefulSets(set.Namespace).Get(set.Name)
		if err != nil {
			t.Fatalf("%s: %s", test.name, err)
		}
		if err := test.validateRollback(set, pods); err != nil {
			t.Fatalf("%s: %s", test.name, err)
		}
	}

	tests := []testcase{
		{
			name:       "monotonic image update",
			invariants: assertMonotonicInvariants,
			initial: func() *apps.StatefulSet {
				return newStatefulSet(3)
			},
			update: func(set *apps.StatefulSet) *apps.StatefulSet {
				set.Spec.Template.Spec.Containers[0].Image = "foo"
				return set
			},
			validateUpdate: func(set *apps.StatefulSet, pods []*v1.Pod) error {
				sort.Sort(ascendingOrdinal(pods))
				for i := range pods {
					if pods[i].Spec.Containers[0].Image != "foo" {
						return fmt.Errorf("want pod %s image foo found %s", pods[i].Name, pods[i].Spec.Containers[0].Image)
					}
				}
				return nil
			},
			validateRollback: func(set *apps.StatefulSet, pods []*v1.Pod) error {
				sort.Sort(ascendingOrdinal(pods))
				for i := range pods {
					if pods[i].Spec.Containers[0].Image != originalImage {
						return fmt.Errorf("want pod %s image %s found %s", pods[i].Name, originalImage, pods[i].Spec.Containers[0].Image)
					}
				}
				return nil
			},
		},
		{
			name:       "monotonic image update and scale up",
			invariants: assertMonotonicInvariants,
			initial: func() *apps.StatefulSet {
				return newStatefulSet(3)
			},
			update: func(set *apps.StatefulSet) *apps.StatefulSet {
				*set.Spec.Replicas = 5
				set.Spec.Template.Spec.Containers[0].Image = "foo"
				return set
			},
			validateUpdate: func(set *apps.StatefulSet, pods []*v1.Pod) error {
				sort.Sort(ascendingOrdinal(pods))
				for i := range pods {
					if pods[i].Spec.Containers[0].Image != "foo" {
						return fmt.Errorf("want pod %s image foo found %s", pods[i].Name, pods[i].Spec.Containers[0].Image)
					}
				}
				return nil
			},
			validateRollback: func(set *apps.StatefulSet, pods []*v1.Pod) error {
				sort.Sort(ascendingOrdinal(pods))
				for i := range pods {
					if pods[i].Spec.Containers[0].Image != originalImage {
						return fmt.Errorf("want pod %s image %s found %s", pods[i].Name, originalImage, pods[i].Spec.Containers[0].Image)
					}
				}
				return nil
			},
		},
		{
			name:       "monotonic image update and scale down",
			invariants: assertMonotonicInvariants,
			initial: func() *apps.StatefulSet {
				return newStatefulSet(5)
			},
			update: func(set *apps.StatefulSet) *apps.StatefulSet {
				*set.Spec.Replicas = 3
				set.Spec.Template.Spec.Containers[0].Image = "foo"
				return set
			},
			validateUpdate: func(set *apps.StatefulSet, pods []*v1.Pod) error {
				sort.Sort(ascendingOrdinal(pods))
				for i := range pods {
					if pods[i].Spec.Containers[0].Image != "foo" {
						return fmt.Errorf("want pod %s image foo found %s", pods[i].Name, pods[i].Spec.Containers[0].Image)
					}
				}
				return nil
			},
			validateRollback: func(set *apps.StatefulSet, pods []*v1.Pod) error {
				sort.Sort(ascendingOrdinal(pods))
				for i := range pods {
					if pods[i].Spec.Containers[0].Image != originalImage {
						return fmt.Errorf("want pod %s image %s found %s", pods[i].Name, originalImage, pods[i].Spec.Containers[0].Image)
					}
				}
				return nil
			},
		},
		{
			name:       "burst image update",
			invariants: assertBurstInvariants,
			initial: func() *apps.StatefulSet {
				return burst(newStatefulSet(3))
			},
			update: func(set *apps.StatefulSet) *apps.StatefulSet {
				set.Spec.Template.Spec.Containers[0].Image = "foo"
				return set
			},
			validateUpdate: func(set *apps.StatefulSet, pods []*v1.Pod) error {
				sort.Sort(ascendingOrdinal(pods))
				for i := range pods {
					if pods[i].Spec.Containers[0].Image != "foo" {
						return fmt.Errorf("want pod %s image foo found %s", pods[i].Name, pods[i].Spec.Containers[0].Image)
					}
				}
				return nil
			},
			validateRollback: func(set *apps.StatefulSet, pods []*v1.Pod) error {
				sort.Sort(ascendingOrdinal(pods))
				for i := range pods {
					if pods[i].Spec.Containers[0].Image != originalImage {
						return fmt.Errorf("want pod %s image %s found %s", pods[i].Name, originalImage, pods[i].Spec.Containers[0].Image)
					}
				}
				return nil
			},
		},
		{
			name:       "burst image update and scale up",
			invariants: assertBurstInvariants,
			initial: func() *apps.StatefulSet {
				return burst(newStatefulSet(3))
			},
			update: func(set *apps.StatefulSet) *apps.StatefulSet {
				*set.Spec.Replicas = 5
				set.Spec.Template.Spec.Containers[0].Image = "foo"
				return set
			},
			validateUpdate: func(set *apps.StatefulSet, pods []*v1.Pod) error {
				sort.Sort(ascendingOrdinal(pods))
				for i := range pods {
					if pods[i].Spec.Containers[0].Image != "foo" {
						return fmt.Errorf("want pod %s image foo found %s", pods[i].Name, pods[i].Spec.Containers[0].Image)
					}
				}
				return nil
			},
			validateRollback: func(set *apps.StatefulSet, pods []*v1.Pod) error {
				sort.Sort(ascendingOrdinal(pods))
				for i := range pods {
					if pods[i].Spec.Containers[0].Image != originalImage {
						return fmt.Errorf("want pod %s image %s found %s", pods[i].Name, originalImage, pods[i].Spec.Containers[0].Image)
					}
				}
				return nil
			},
		},
		{
			name:       "burst image update and scale down",
			invariants: assertBurstInvariants,
			initial: func() *apps.StatefulSet {
				return burst(newStatefulSet(5))
			},
			update: func(set *apps.StatefulSet) *apps.StatefulSet {
				*set.Spec.Replicas = 3
				set.Spec.Template.Spec.Containers[0].Image = "foo"
				return set
			},
			validateUpdate: func(set *apps.StatefulSet, pods []*v1.Pod) error {
				sort.Sort(ascendingOrdinal(pods))
				for i := range pods {
					if pods[i].Spec.Containers[0].Image != "foo" {
						return fmt.Errorf("want pod %s image foo found %s", pods[i].Name, pods[i].Spec.Containers[0].Image)
					}
				}
				return nil
			},
			validateRollback: func(set *apps.StatefulSet, pods []*v1.Pod) error {
				sort.Sort(ascendingOrdinal(pods))
				for i := range pods {
					if pods[i].Spec.Containers[0].Image != originalImage {
						return fmt.Errorf("want pod %s image %s found %s", pods[i].Name, originalImage, pods[i].Spec.Containers[0].Image)
					}
				}
				return nil
			},
		},
	}
	runTestOverPVCDeletePolicies(t, "", func(t *testing.T, policy apps.PersistentVolumeClaimDeletePolicyType) {
		for i := range tests {
			testFn(t, &tests[i], policy)
		}
	})
}

func TestStatefulSetAvailability(t *testing.T) {
	tests := []struct {
		name                              string
		inputSTS                          *apps.StatefulSet
		expectedActiveReplicas            int32
		readyDuration                     time.Duration
		minReadySecondsFeaturegateEnabled bool
	}{
		{
			name: "replicas not running for required time, still will be available," +
				" when minReadySeconds is disabled",
			inputSTS:                          setMinReadySeconds(newStatefulSet(1), int32(3600)),
			readyDuration:                     0 * time.Minute,
			expectedActiveReplicas:            int32(1),
			minReadySecondsFeaturegateEnabled: false,
		},
		{
			name:                              "replicas running for required time, when minReadySeconds is enabled",
			inputSTS:                          setMinReadySeconds(newStatefulSet(1), int32(3600)),
			readyDuration:                     -120 * time.Minute,
			expectedActiveReplicas:            int32(1),
			minReadySecondsFeaturegateEnabled: true,
		},
		{
			name:                              "replicas not running for required time, when minReadySeconds is enabled",
			inputSTS:                          setMinReadySeconds(newStatefulSet(1), int32(3600)),
			readyDuration:                     -30 * time.Minute,
			expectedActiveReplicas:            int32(0),
			minReadySecondsFeaturegateEnabled: true,
		},
	}
	for _, test := range tests {
		defer featuregatetesting.SetFeatureGateDuringTest(t, utilfeature.DefaultFeatureGate, features.StatefulSetMinReadySeconds, test.minReadySecondsFeaturegateEnabled)()
		set := test.inputSTS
		client := fake.NewSimpleClientset(set)
		spc, _, ssc, stop := setupController(client)
		defer close(stop)
		if err := scaleUpStatefulSetControl(set, ssc, spc, assertBurstInvariants); err != nil {
			t.Fatalf("%s: %s", test.name, err)
		}
		selector, err := metav1.LabelSelectorAsSelector(set.Spec.Selector)
		if err != nil {
			t.Fatalf("%s: %s", test.name, err)
		}
		_, err = spc.podsLister.Pods(set.Namespace).List(selector)
		if err != nil {
			t.Fatalf("%s: %s", test.name, err)
		}
		set, err = spc.setsLister.StatefulSets(set.Namespace).Get(set.Name)
		if err != nil {
			t.Fatalf("%s: %s", test.name, err)
		}
		pods, err := spc.setPodAvailable(set, 0, time.Now().Add(test.readyDuration))
		if err != nil {
			t.Fatalf("%s: %s", test.name, err)
		}
		status, err := ssc.UpdateStatefulSet(set, pods)
		if err != nil {
			t.Fatalf("%s: %s", test.name, err)
		}
		if status.AvailableReplicas != test.expectedActiveReplicas {
			t.Fatalf("expected %d active replicas got %d", test.expectedActiveReplicas, status.AvailableReplicas)
		}
	}
}

type requestTracker struct {
	requests int
	err      error
	after    int
}

func (rt *requestTracker) errorReady() bool {
	return rt.err != nil && rt.requests >= rt.after
}

func (rt *requestTracker) inc() {
	rt.requests++
}

func (rt *requestTracker) reset() {
	rt.err = nil
	rt.after = 0
}

type fakeObjectManager struct {
	podsLister       corelisters.PodLister
	claimsLister     corelisters.PersistentVolumeClaimLister
	setsLister       appslisters.StatefulSetLister
	podsIndexer      cache.Indexer
	claimsIndexer    cache.Indexer
	setsIndexer      cache.Indexer
	revisionsIndexer cache.Indexer
	createPodTracker requestTracker
	updatePodTracker requestTracker
	deletePodTracker requestTracker
}

func newFakeObjectManager(podInformer coreinformers.PodInformer, setInformer appsinformers.StatefulSetInformer, revisionInformer appsinformers.ControllerRevisionInformer) *fakeObjectManager {
	claimsIndexer := cache.NewIndexer(controller.KeyFunc, cache.Indexers{cache.NamespaceIndex: cache.MetaNamespaceIndexFunc})
	return &fakeObjectManager{
		podInformer.Lister(),
		corelisters.NewPersistentVolumeClaimLister(claimsIndexer),
		setInformer.Lister(),
		podInformer.Informer().GetIndexer(),
		claimsIndexer,
		setInformer.Informer().GetIndexer(),
		revisionInformer.Informer().GetIndexer(),
		requestTracker{0, nil, 0},
		requestTracker{0, nil, 0},
		requestTracker{0, nil, 0}}
}

func (om *fakeObjectManager) CreatePod(pod *v1.Pod) error {
	defer om.createPodTracker.inc()
	if om.createPodTracker.errorReady() {
		defer om.createPodTracker.reset()
		return om.createPodTracker.err
	}
	pod.SetUID(types.UID(pod.Name + "-uid"))
	return om.podsIndexer.Update(pod)
}

func (om *fakeObjectManager) GetPod(namespace, podName string) (*v1.Pod, error) {
	return om.podsLister.Pods(namespace).Get(podName)
}

func (om *fakeObjectManager) UpdatePod(pod *v1.Pod) error {
	return om.podsIndexer.Update(pod)
}

func (om *fakeObjectManager) DeletePod(pod *v1.Pod) error {
	defer om.deletePodTracker.inc()
	if om.deletePodTracker.errorReady() {
		defer om.deletePodTracker.reset()
		return om.deletePodTracker.err
	}
	if key, err := controller.KeyFunc(pod); err != nil {
		return err
	} else if obj, found, err := om.podsIndexer.GetByKey(key); err != nil {
		return err
	} else if found {
		return om.podsIndexer.Delete(obj)
	}
	return nil // Not found, no error in deleting.
}

func (om *fakeObjectManager) CreateClaim(claim *v1.PersistentVolumeClaim) error {
	om.claimsIndexer.Update(claim)
	return nil
}

func (om *fakeObjectManager) GetClaim(namespace, claimName string) (*v1.PersistentVolumeClaim, error) {
	return om.claimsLister.PersistentVolumeClaims(namespace).Get(claimName)
}

func (om *fakeObjectManager) UpdateClaim(claim *v1.PersistentVolumeClaim) error {
	// Validate ownerRefs.
	refs := claim.GetOwnerReferences()
	for _, ref := range refs {
		if ref.APIVersion == "" || ref.Kind == "" || ref.Name == "" {
			return fmt.Errorf("invalid ownerRefs: %s %v", claim.Name, refs)
		}
	}
	om.claimsIndexer.Update(claim)
	return nil
}

func (om *fakeObjectManager) SetCreateStatefulPodError(err error, after int) {
	om.createPodTracker.err = err
	om.createPodTracker.after = after
}

func (om *fakeObjectManager) SetUpdateStatefulPodError(err error, after int) {
	om.updatePodTracker.err = err
	om.updatePodTracker.after = after
}

func (om *fakeObjectManager) SetDeleteStatefulPodError(err error, after int) {
	om.deletePodTracker.err = err
	om.deletePodTracker.after = after
}

func (om *fakeObjectManager) setPodPending(set *apps.StatefulSet, ordinal int) ([]*v1.Pod, error) {
	selector, err := metav1.LabelSelectorAsSelector(set.Spec.Selector)
	if err != nil {
		return nil, err
	}
	pods, err := om.podsLister.Pods(set.Namespace).List(selector)
	if err != nil {
		return nil, err
	}
	if 0 > ordinal || ordinal >= len(pods) {
		return nil, fmt.Errorf("ordinal %d out of range [0,%d)", ordinal, len(pods))
	}
	sort.Sort(ascendingOrdinal(pods))
	pod := pods[ordinal].DeepCopy()
	pod.Status.Phase = v1.PodPending
	fakeResourceVersion(pod)
	om.podsIndexer.Update(pod)
	return om.podsLister.Pods(set.Namespace).List(selector)
}

func (om *fakeObjectManager) setPodRunning(set *apps.StatefulSet, ordinal int) ([]*v1.Pod, error) {
	selector, err := metav1.LabelSelectorAsSelector(set.Spec.Selector)
	if err != nil {
		return nil, err
	}
	pods, err := om.podsLister.Pods(set.Namespace).List(selector)
	if err != nil {
		return nil, err
	}
	if 0 > ordinal || ordinal >= len(pods) {
		return nil, fmt.Errorf("ordinal %d out of range [0,%d)", ordinal, len(pods))
	}
	sort.Sort(ascendingOrdinal(pods))
	pod := pods[ordinal].DeepCopy()
	pod.Status.Phase = v1.PodRunning
	fakeResourceVersion(pod)
	om.podsIndexer.Update(pod)
	return om.podsLister.Pods(set.Namespace).List(selector)
}

func (om *fakeObjectManager) setPodReady(set *apps.StatefulSet, ordinal int) ([]*v1.Pod, error) {
	selector, err := metav1.LabelSelectorAsSelector(set.Spec.Selector)
	if err != nil {
		return nil, err
	}
	pods, err := om.podsLister.Pods(set.Namespace).List(selector)
	if err != nil {
		return nil, err
	}
	if 0 > ordinal || ordinal >= len(pods) {
		return nil, fmt.Errorf("ordinal %d out of range [0,%d)", ordinal, len(pods))
	}
	sort.Sort(ascendingOrdinal(pods))
	pod := pods[ordinal].DeepCopy()
	condition := v1.PodCondition{Type: v1.PodReady, Status: v1.ConditionTrue}
	podutil.UpdatePodCondition(&pod.Status, &condition)
	fakeResourceVersion(pod)
	om.podsIndexer.Update(pod)
	return om.podsLister.Pods(set.Namespace).List(selector)
}

func (om *fakeObjectManager) setPodAvailable(set *apps.StatefulSet, ordinal int, lastTransitionTime time.Time) ([]*v1.Pod, error) {
	selector, err := metav1.LabelSelectorAsSelector(set.Spec.Selector)
	if err != nil {
		return nil, err
	}
	pods, err := om.podsLister.Pods(set.Namespace).List(selector)
	if err != nil {
		return nil, err
	}
	if 0 > ordinal || ordinal >= len(pods) {
		return nil, fmt.Errorf("ordinal %d out of range [0,%d)", ordinal, len(pods))
	}
	sort.Sort(ascendingOrdinal(pods))
	pod := pods[ordinal].DeepCopy()
	condition := v1.PodCondition{Type: v1.PodReady, Status: v1.ConditionTrue, LastTransitionTime: metav1.Time{Time: lastTransitionTime}}
	_, existingCondition := podutil.GetPodCondition(&pod.Status, condition.Type)
	if existingCondition != nil {
		existingCondition.Status = v1.ConditionTrue
		existingCondition.LastTransitionTime = metav1.Time{Time: lastTransitionTime}
	} else {
		existingCondition = &v1.PodCondition{
			Type:               v1.PodReady,
			Status:             v1.ConditionTrue,
			LastTransitionTime: metav1.Time{Time: lastTransitionTime},
		}
		pod.Status.Conditions = append(pod.Status.Conditions, *existingCondition)
	}
	podutil.UpdatePodCondition(&pod.Status, &condition)
	fakeResourceVersion(pod)
	om.podsIndexer.Update(pod)
	return om.podsLister.Pods(set.Namespace).List(selector)
}

func (om *fakeObjectManager) addTerminatingPod(set *apps.StatefulSet, ordinal int) ([]*v1.Pod, error) {
	pod := newStatefulSetPod(set, ordinal)
	pod.SetUID(types.UID(pod.Name + "-uid")) // To match fakeObjectManager.CreatePod
	pod.Status.Phase = v1.PodRunning
	deleted := metav1.NewTime(time.Now())
	pod.DeletionTimestamp = &deleted
	condition := v1.PodCondition{Type: v1.PodReady, Status: v1.ConditionTrue}
	fakeResourceVersion(pod)
	podutil.UpdatePodCondition(&pod.Status, &condition)
	om.podsIndexer.Update(pod)
	selector, err := metav1.LabelSelectorAsSelector(set.Spec.Selector)
	if err != nil {
		return nil, err
	}
	return om.podsLister.Pods(set.Namespace).List(selector)
}

func (om *fakeObjectManager) setPodTerminated(set *apps.StatefulSet, ordinal int) ([]*v1.Pod, error) {
	pod := newStatefulSetPod(set, ordinal)
	deleted := metav1.NewTime(time.Now())
	pod.DeletionTimestamp = &deleted
	fakeResourceVersion(pod)
	om.podsIndexer.Update(pod)
	selector, err := metav1.LabelSelectorAsSelector(set.Spec.Selector)
	if err != nil {
		return nil, err
	}
	return om.podsLister.Pods(set.Namespace).List(selector)
}

var _ StatefulPodControlObjectManager = &fakeObjectManager{}

type fakeStatefulSetStatusUpdater struct {
	setsLister          appslisters.StatefulSetLister
	setsIndexer         cache.Indexer
	updateStatusTracker requestTracker
}

func newFakeStatefulSetStatusUpdater(setInformer appsinformers.StatefulSetInformer) *fakeStatefulSetStatusUpdater {
	return &fakeStatefulSetStatusUpdater{
		setInformer.Lister(),
		setInformer.Informer().GetIndexer(),
		requestTracker{0, nil, 0},
	}
}

func (ssu *fakeStatefulSetStatusUpdater) UpdateStatefulSetStatus(set *apps.StatefulSet, status *apps.StatefulSetStatus) error {
	defer ssu.updateStatusTracker.inc()
	if ssu.updateStatusTracker.errorReady() {
		defer ssu.updateStatusTracker.reset()
		return ssu.updateStatusTracker.err
	}
	set.Status = *status
	ssu.setsIndexer.Update(set)
	return nil
}

func (ssu *fakeStatefulSetStatusUpdater) SetUpdateStatefulSetStatusError(err error, after int) {
	ssu.updateStatusTracker.err = err
	ssu.updateStatusTracker.after = after
}

var _ StatefulSetStatusUpdaterInterface = &fakeStatefulSetStatusUpdater{}

func assertMonotonicInvariants(set *apps.StatefulSet, om *fakeObjectManager) error {
	selector, err := metav1.LabelSelectorAsSelector(set.Spec.Selector)
	if err != nil {
		return err
	}
	pods, err := om.podsLister.Pods(set.Namespace).List(selector)
	if err != nil {
		return err
	}
	sort.Sort(ascendingOrdinal(pods))
	for ord := 0; ord < len(pods); ord++ {
		if ord > 0 && isRunningAndReady(pods[ord]) && !isRunningAndReady(pods[ord-1]) {
			return fmt.Errorf("successor %s is Running and Ready while %s is not", pods[ord].Name, pods[ord-1].Name)
		}

		if getOrdinal(pods[ord]) != ord {
			return fmt.Errorf("pods %s deployed in the wrong order %d", pods[ord].Name, ord)
		}

		if !storageMatches(set, pods[ord]) {
			return fmt.Errorf("pods %s does not match the storage specification of StatefulSet %s ", pods[ord].Name, set.Name)
		}

		for _, claim := range getPersistentVolumeClaims(set, pods[ord]) {
			claim, _ := om.claimsLister.PersistentVolumeClaims(set.Namespace).Get(claim.Name)
			if err := checkClaimInvarients(set, pods[ord], claim, ord); err != nil {
				return err
			}
		}

		if !identityMatches(set, pods[ord]) {
			return fmt.Errorf("pods %s does not match the identity specification of StatefulSet %s ", pods[ord].Name, set.Name)
		}
	}
	return nil
}

func assertBurstInvariants(set *apps.StatefulSet, om *fakeObjectManager) error {
	selector, err := metav1.LabelSelectorAsSelector(set.Spec.Selector)
	if err != nil {
		return err
	}
	pods, err := om.podsLister.Pods(set.Namespace).List(selector)
	if err != nil {
		return err
	}
	sort.Sort(ascendingOrdinal(pods))
	for ord := 0; ord < len(pods); ord++ {
		if !storageMatches(set, pods[ord]) {
			return fmt.Errorf("pods %s does not match the storage specification of StatefulSet %s ", pods[ord].Name, set.Name)
		}

		for _, claim := range getPersistentVolumeClaims(set, pods[ord]) {
			claim, err := om.claimsLister.PersistentVolumeClaims(set.Namespace).Get(claim.Name)
			if err != nil {
				return err
			}
			if err := checkClaimInvarients(set, pods[ord], claim, ord); err != nil {
				return err
			}
		}

		if !identityMatches(set, pods[ord]) {
			return fmt.Errorf("pods %s does not match the identity specification of StatefulSet %s ",
				pods[ord].Name,
				set.Name)
		}
	}
	return nil
}

func assertUpdateInvariants(set *apps.StatefulSet, om *fakeObjectManager) error {
	selector, err := metav1.LabelSelectorAsSelector(set.Spec.Selector)
	if err != nil {
		return err
	}
	pods, err := om.podsLister.Pods(set.Namespace).List(selector)
	if err != nil {
		return err
	}
	sort.Sort(ascendingOrdinal(pods))
	for ord := 0; ord < len(pods); ord++ {

		if !storageMatches(set, pods[ord]) {
			return fmt.Errorf("pod %s does not match the storage specification of StatefulSet %s ", pods[ord].Name, set.Name)
		}

		for _, claim := range getPersistentVolumeClaims(set, pods[ord]) {
			claim, err := om.claimsLister.PersistentVolumeClaims(set.Namespace).Get(claim.Name)
			if err != nil {
				return err
			}
			if err := checkClaimInvarients(set, pods[ord], claim, ord); err != nil {
				return err
			}
		}

		if !identityMatches(set, pods[ord]) {
			return fmt.Errorf("pod %s does not match the identity specification of StatefulSet %s ", pods[ord].Name, set.Name)
		}
	}
	if set.Spec.UpdateStrategy.Type == apps.OnDeleteStatefulSetStrategyType {
		return nil
	}
	if set.Spec.UpdateStrategy.Type == apps.RollingUpdateStatefulSetStrategyType {
		for i := 0; i < int(set.Status.CurrentReplicas) && i < len(pods); i++ {
			if want, got := set.Status.CurrentRevision, getPodRevision(pods[i]); want != got {
				return fmt.Errorf("pod %s want current revision %s got %s", pods[i].Name, want, got)
			}
		}
		for i, j := len(pods)-1, 0; j < int(set.Status.UpdatedReplicas); i, j = i-1, j+1 {
			if want, got := set.Status.UpdateRevision, getPodRevision(pods[i]); want != got {
				return fmt.Errorf("pod %s want update revision %s got %s", pods[i].Name, want, got)
			}
		}
	}
	return nil
}

func checkClaimInvarients(set *apps.StatefulSet, pod *v1.Pod, claim *v1.PersistentVolumeClaim, ordinal int) error {
	policy := apps.RetainPersistentVolumeClaimDeletePolicyType
	if set.Spec.PersistentVolumeClaimDeletePolicy != nil && utilfeature.DefaultFeatureGate.Enabled(features.StatefulSetAutoDeletePVC) {
		policy = *set.Spec.PersistentVolumeClaimDeletePolicy
	}
	claimShouldBeRetained := policy == apps.RetainPersistentVolumeClaimDeletePolicyType || policy == apps.DeleteOnStatefulSetDeletionOnlyPersistentVolumeClaimDeletePolicyType
	if claim == nil {
		if claimShouldBeRetained {
			return fmt.Errorf("claim %s for Pod %s was not created", claim.Name, pod)
		}
		return nil // A non-retained claim has no invariants to satisfy.
	}

	if pod.Status.Phase != v1.PodRunning || !podutil.IsPodReady(pod) {
		// The pod has spun up yet, we do not expect the owner refs on the claim to have been set.
		return nil
	}

	switch policy {
	case apps.RetainPersistentVolumeClaimDeletePolicyType:
		if hasOwnerRef(claim, set) {
			return fmt.Errorf("claim %s has unexpected owner ref on %s for StatefulSet retain", claim.Name, set.Name)
		}
		if hasOwnerRef(claim, pod) {
			return fmt.Errorf("claim %s has unexpected owner ref on pod %s for StatefulSet retain", claim.Name, pod.Name)
		}
	case apps.DeleteOnStatefulSetDeletionOnlyPersistentVolumeClaimDeletePolicyType:
		if !hasOwnerRef(claim, set) {
			return fmt.Errorf("claim %s does not have owner ref on %s for StatefulSet deletion", claim.Name, set.Name)
		}
		if hasOwnerRef(claim, pod) {
			return fmt.Errorf("claim %s has unexpected owner ref on pod %s for StatefulSet deletion", claim.Name, pod.Name)
		}
	case apps.DeleteOnScaledownOnlyPersistentVolumeClaimDeletePolicyType:
		if hasOwnerRef(claim, set) {
			return fmt.Errorf("claim %s has unexpected owner ref on %s for scaledown only", claim.Name, set.Name)
		}
		if ordinal >= int(*set.Spec.Replicas) && !hasOwnerRef(claim, pod) {
			return fmt.Errorf("claim %s does not have owner ref on condemned pod %s for scaledown delete", claim.Name, pod.Name)
		}
		if ordinal < int(*set.Spec.Replicas) && hasOwnerRef(claim, pod) {
			return fmt.Errorf("claim %s has unexpected owner ref on condemned pod %s for scaledown delete", claim.Name, pod.Name)
		}
	case apps.DeleteOnScaledownAndStatefulSetDeletionPersistentVolumeClaimDeletePolicyType:
		if ordinal >= int(*set.Spec.Replicas) {
			if !hasOwnerRef(claim, pod) || hasOwnerRef(claim, set) {
				return fmt.Errorf("condemned claim %s has bad owner refs: %v", claim.Name, claim.GetOwnerReferences())
			}
		} else {
			if hasOwnerRef(claim, pod) || !hasOwnerRef(claim, set) {
				return fmt.Errorf("live claim %s has bad owner refs: %v", claim.Name, claim.GetOwnerReferences())
			}
		}
	}
	return nil
}

func fakeResourceVersion(object interface{}) {
	obj, isObj := object.(metav1.Object)
	if !isObj {
		return
	}
	if version := obj.GetResourceVersion(); version == "" {
		obj.SetResourceVersion("1")
	} else if intValue, err := strconv.ParseInt(version, 10, 32); err == nil {
		obj.SetResourceVersion(strconv.FormatInt(intValue+1, 10))
	}
}

func scaleUpStatefulSetControl(set *apps.StatefulSet,
	ssc StatefulSetControlInterface,
	om *fakeObjectManager,
	invariants invariantFunc) error {
	selector, err := metav1.LabelSelectorAsSelector(set.Spec.Selector)
	if err != nil {
		return err
	}
	for set.Status.ReadyReplicas < *set.Spec.Replicas {
		pods, err := om.podsLister.Pods(set.Namespace).List(selector)
		if err != nil {
			return err
		}
		sort.Sort(ascendingOrdinal(pods))

		// ensure all pods are valid (have a phase)
		for ord, pod := range pods {
			if pod.Status.Phase == "" {
				if pods, err = om.setPodPending(set, ord); err != nil {
					return err
				}
				break
			}
		}

		// select one of the pods and move it forward in status
		if len(pods) > 0 {
			ord := int(rand.Int63n(int64(len(pods))))
			pod := pods[ord]
			switch pod.Status.Phase {
			case v1.PodPending:
				if pods, err = om.setPodRunning(set, ord); err != nil {
					return err
				}
			case v1.PodRunning:
				if pods, err = om.setPodReady(set, ord); err != nil {
					return err
				}
			default:
				continue
			}
		}
		// run the controller once and check invariants
		_, err = ssc.UpdateStatefulSet(set, pods)
		if err != nil {
			return err
		}
		set, err = om.setsLister.StatefulSets(set.Namespace).Get(set.Name)
		if err != nil {
			return err
		}
		if err := invariants(set, om); err != nil {
			return err
		}
		//fmt.Printf("Ravig pod conditions %v %v", set.Status.ReadyReplicas, *set.Spec.Replicas)
	}
	return invariants(set, om)
}

func scaleDownStatefulSetControl(set *apps.StatefulSet, ssc StatefulSetControlInterface, om *fakeObjectManager, invariants invariantFunc) error {
	selector, err := metav1.LabelSelectorAsSelector(set.Spec.Selector)
	if err != nil {
		return err
	}

	for set.Status.Replicas > *set.Spec.Replicas {
		pods, err := om.podsLister.Pods(set.Namespace).List(selector)
		if err != nil {
			return err
		}
		sort.Sort(ascendingOrdinal(pods))
		if ordinal := len(pods) - 1; ordinal >= 0 {
			if _, err := ssc.UpdateStatefulSet(set, pods); err != nil {
				return err
			}
			set, err = om.setsLister.StatefulSets(set.Namespace).Get(set.Name)
			if err != nil {
				return err
			}
			if pods, err = om.addTerminatingPod(set, ordinal); err != nil {
				return err
			}
			if _, err = ssc.UpdateStatefulSet(set, pods); err != nil {
				return err
			}
			set, err = om.setsLister.StatefulSets(set.Namespace).Get(set.Name)
			if err != nil {
				return err
			}
			pods, err = om.podsLister.Pods(set.Namespace).List(selector)
			if err != nil {
				return err
			}
			sort.Sort(ascendingOrdinal(pods))

			if len(pods) > 0 {
				om.podsIndexer.Delete(pods[len(pods)-1])
			}
		}
		if _, err := ssc.UpdateStatefulSet(set, pods); err != nil {
			return err
		}
		set, err = om.setsLister.StatefulSets(set.Namespace).Get(set.Name)
		if err != nil {
			return err
		}

		if err := invariants(set, om); err != nil {
			return err
		}
	}
	// If there are claims with ownerRefs on pods that have been deleted, delete them.
	pods, err := om.podsLister.Pods(set.Namespace).List(selector)
	if err != nil {
		return err
	}
	currentPods := map[string]bool{}
	for _, pod := range pods {
		currentPods[pod.Name] = true
	}
	claims, err := om.claimsLister.PersistentVolumeClaims(set.Namespace).List(selector)
	if err != nil {
		return err
	}
	for _, claim := range claims {
		claimPodName := getClaimPodName(set, claim)
		if claimPodName == "" {
			continue // Skip claims not related to a stateful set pod.
		}
		if _, found := currentPods[claimPodName]; found {
			continue // Skip claims which still have a current pod.
		}
		for _, refs := range claim.GetOwnerReferences() {
			if refs.Name == claimPodName {
				om.claimsIndexer.Delete(claim)
				break
			}
		}
	}

	return invariants(set, om)
}

func updateComplete(set *apps.StatefulSet, pods []*v1.Pod) bool {
	sort.Sort(ascendingOrdinal(pods))
	if len(pods) != int(*set.Spec.Replicas) {
		return false
	}
	if set.Status.ReadyReplicas != *set.Spec.Replicas {
		return false
	}

	switch set.Spec.UpdateStrategy.Type {
	case apps.OnDeleteStatefulSetStrategyType:
		return true
	case apps.RollingUpdateStatefulSetStrategyType:
		if set.Spec.UpdateStrategy.RollingUpdate == nil || *set.Spec.UpdateStrategy.RollingUpdate.Partition <= 0 {
			if set.Status.CurrentReplicas < *set.Spec.Replicas {
				return false
			}
			for i := range pods {
				if getPodRevision(pods[i]) != set.Status.CurrentRevision {
					return false
				}
			}
		} else {
			partition := int(*set.Spec.UpdateStrategy.RollingUpdate.Partition)
			if len(pods) < partition {
				return false
			}
			for i := partition; i < len(pods); i++ {
				if getPodRevision(pods[i]) != set.Status.UpdateRevision {
					return false
				}
			}
		}
	}
	return true
}

func updateStatefulSetControl(set *apps.StatefulSet,
	ssc StatefulSetControlInterface,
	om *fakeObjectManager,
	invariants invariantFunc) error {
	selector, err := metav1.LabelSelectorAsSelector(set.Spec.Selector)
	if err != nil {
		return err
	}
	pods, err := om.podsLister.Pods(set.Namespace).List(selector)
	if err != nil {
		return err
	}
	if _, err = ssc.UpdateStatefulSet(set, pods); err != nil {
		return err
	}

	set, err = om.setsLister.StatefulSets(set.Namespace).Get(set.Name)
	if err != nil {
		return err
	}
	pods, err = om.podsLister.Pods(set.Namespace).List(selector)
	if err != nil {
		return err
	}
	for !updateComplete(set, pods) {
		pods, err = om.podsLister.Pods(set.Namespace).List(selector)
		if err != nil {
			return err
		}
		sort.Sort(ascendingOrdinal(pods))
		initialized := false
		for ord, pod := range pods {
			if pod.Status.Phase == "" {
				if pods, err = om.setPodPending(set, ord); err != nil {
					return err
				}
				break
			}
		}
		if initialized {
			continue
		}

		if len(pods) > 0 {
			ord := int(rand.Int63n(int64(len(pods))))
			pod := pods[ord]
			switch pod.Status.Phase {
			case v1.PodPending:
				if pods, err = om.setPodRunning(set, ord); err != nil {
					return err
				}
			case v1.PodRunning:
				if pods, err = om.setPodReady(set, ord); err != nil {
					return err
				}
			default:
				continue
			}
		}

		if _, err = ssc.UpdateStatefulSet(set, pods); err != nil {
			return err
		}
		set, err = om.setsLister.StatefulSets(set.Namespace).Get(set.Name)
		if err != nil {
			return err
		}
		if err := invariants(set, om); err != nil {
			return err
		}
		pods, err = om.podsLister.Pods(set.Namespace).List(selector)
		if err != nil {
			return err
		}
	}
	return invariants(set, om)
}

func newRevisionOrDie(set *apps.StatefulSet, revision int64) *apps.ControllerRevision {
	rev, err := newRevision(set, revision, set.Status.CollisionCount)
	if err != nil {
		panic(err)
	}
	return rev
}

func isOrHasInternalError(err error) bool {
	agg, ok := err.(utilerrors.Aggregate)
	return !ok && !apierrors.IsInternalError(err) || ok && len(agg.Errors()) > 0 && !apierrors.IsInternalError(agg.Errors()[0])
}
