(ns clj-datastore.file-test
  (:require [clojure.java.io :as io])
  (:use [clojure.test]
        [clj-datastore.file]))

(def test-file-name "__test_data_store_")
(def test-file-name-full (str test-file-name ".edn"))
(defn delete-test-file []
  (io/delete-file test-file-name-full true))

(def prepared-last-modified (.. (java.util.Date.) getTime))
(defn prepare-test-file []
  (delete-test-file)
  (spit test-file-name-full {:recs [{:foo :bar}] :last-modified prepared-last-modified})
  )

(defn test-fixture [f]
  (prepare-test-file)
  (f)
  (delete-test-file)
  )

(use-fixtures :each test-fixture)

(deftest test-local-storage
  (testing "normal write"
    (let [_ (prepare-test-file)
          ds (atom (clj_datastore.file.DataStore. [:foo] test-file-name local-storage nil nil nil))
          ;; Make sure records are loaded before make-data-store-update is called
          _ (read-records local-storage ds)
          ;; OK to do before with-redefs, since everything needed is known here
          towrite (make-data-store-update ds conj {:foo :bar})
          expected [{:foo :bar} {:foo :bar}]
          _ (Thread/sleep 10)
          expected-last-modified (java.util.Date.)]
      (with-redefs [get-current-datetime (constantly expected-last-modified)]
        (write-records local-storage towrite)
        (let [{:keys [recs last-modified]} (read-string (slurp test-file-name-full))]
          (is (= expected recs))
          (is (= (:store @towrite) recs))
          (is (= (:last-modified @towrite) expected-last-modified))
          (is (not= (:store @ds) recs))
          (is (not= (:last-modified @ds) expected-last-modified))
          ;; Refresh ds from storage
          (do-list-records ds)
          (is (= (:store @ds) recs))
          (is (= (:last-modified @ds) expected-last-modified))
          ))))

  (testing "write before reading (expected retry exception)"
    (let [_ (prepare-test-file)
          ds (atom (clj_datastore.file.DataStore. [:foo] test-file-name local-storage nil nil nil))
          expected [{:foo :bar} {:foo :bar}]
          _ (Thread/sleep 10)
          expected-last-modified (java.util.Date.)]
      (with-redefs [get-current-datetime (constantly expected-last-modified)]
        (try
          ;; Attempt to write..should throw an exception since we havent loaded yet
          (write-records local-storage (make-data-store-update ds conj {:foo :bar}))
          (is false "Expected RetryWrite exception")
          (catch clojure.lang.ExceptionInfo e
            (is (= :retry-write (-> (ex-data e)
                                    :cause)))
            (read-records local-storage ds)
            (let [towrite (make-data-store-update ds conj {:foo :bar})
                  _ (write-records local-storage towrite)
                  {:keys [recs last-modified]} (read-string (slurp test-file-name-full))]
              (is (= expected recs))
              (is (= (:store @towrite) recs))
              (is (= (:last-modified @towrite) expected-last-modified))
              (is (not= (:store @ds) recs))
              (is (not= (:last-modified @ds) expected-last-modified))
              ;; Refresh ds from storage
              (read-records local-storage ds)
              (is (= (:store @ds) recs))
              (is (= (:last-modified @ds) expected-last-modified))
              )))))
    )
  (testing "concurrent write exception"
    (let [_   (prepare-test-file)
          ds  (atom (clj_datastore.file.DataStore. [:foo] test-file-name local-storage nil nil nil))
          dsb (atom (clj_datastore.file.DataStore. [:foo] test-file-name local-storage nil nil nil))
          expected-recs [{:foo :bar} {:foo :bar}]
          expected-recsb [{:foo :bar} {:foo :bar} {:foo :baz}]
          _ (Thread/sleep 10)
          expected-last-modified (atom (java.util.Date.))
          towrite (atom nil)
          actual-file (atom nil)]
      ;; loads the records for both
      (read-records local-storage ds)
      (read-records local-storage dsb)
      ;; writes an update to one
      (with-redefs [get-current-datetime #(reset! expected-last-modified (java.util.Date.))]
        ;; First, write to ds, which should succeed
        (reset! towrite @(make-data-store-update ds conj {:foo :bar}))
        (write-records local-storage towrite)
        (reset! actual-file (read-string (slurp test-file-name-full)))
        (let [{:keys [recs last-modified]} @actual-file]
          (is (= expected-recs recs))
          (is (= (:store @towrite) recs))
          (is (= (:last-modified @towrite) @expected-last-modified))
          ;; Verify that ds and dsb didnt change
          (is (not= (:store @ds) recs))
          (is (not= (:last-modified @ds) @expected-last-modified))
          (is (not= (:store @dsb) recs))
          (is (not= (:last-modified @dsb) @expected-last-modified))
          ;; Refresh ds from storage
          (read-records local-storage ds)
          (is (= (:store @ds) recs))
          (is (= (:last-modified @ds) @expected-last-modified)))

        ;; Next, try a write to dsb, which should fail because it's out of date
        (reset! towrite @(make-data-store-update dsb conj {:foo :baz}))
        (try
          (is (= (:last-modified @dsb) (java.util.Date. prepared-last-modified)))
          (write-records local-storage towrite)
          (is false "Expected RetryWrite exception")
          (catch clojure.lang.ExceptionInfo e
            (is (= :retry-write (-> (ex-data e)
                                    :cause)))
            ;; In this case, neither the update nor the original data store get updated
            (is (not= (:last-modified @towrite) @expected-last-modified))
            (is (= (:last-modified @dsb) (java.util.Date. prepared-last-modified)))
            )
          ))))
  (testing "concurrent write exception retry"
    (let [_   (prepare-test-file)
          ds  (atom (clj_datastore.file.DataStore. [:foo] test-file-name local-storage nil nil nil))
          dsb (atom (clj_datastore.file.DataStore. [:foo] test-file-name local-storage nil nil nil))
          expected-recs [{:foo :bar} {:foo :bar}]
          expected-recsb [{:foo :bar} {:foo :bar} {:foo :baz}]
          _ (Thread/sleep 10)
          expected-last-modified (atom nil)
          towrite (atom nil)
          actual-file (atom nil)]
      ;; loads the records for both
      (read-records local-storage ds)
      (read-records local-storage dsb)
      ;; writes an update to one
      (with-redefs [get-current-datetime #(reset! expected-last-modified (java.util.Date.))]
        ;; First, write to ds, which should succeed
        (reset! towrite @(make-data-store-update ds conj {:foo :bar}))
        (write-records local-storage towrite)
        ;; Refresh ds, so it represents the current file data
        (read-records local-storage ds)
        ;; Next, try a write to dsb, which should fail because it's out of date
        (reset! expected-last-modified (java.util.Date.))
        (reset! towrite @(make-data-store-update dsb conj {:foo :baz}))
        (try
          (write-records local-storage towrite)
          (is false "Expected RetryWrite exception")
          (catch clojure.lang.ExceptionInfo e
            (is (= :retry-write (-> (ex-data e)
                                    :cause)))
            (read-records local-storage dsb)
            (is (= @ds @dsb))
            ;; Retry the write, should succeed since we've reloaded dsb
            (reset! towrite @(make-data-store-update dsb conj {:foo :baz}))
            (write-records local-storage towrite)
            (reset! actual-file (read-string (slurp test-file-name-full)))
            (let [{:keys [recs last-modified]} @actual-file]
              (is (= expected-recsb recs))
              (is (= (:store @towrite) recs))
              (is (= (:last-modified @towrite) @expected-last-modified))
              ;; Verify that ds and dsb didnt change
              (is (not= (:store @ds) recs))
              (is (not= (:last-modified @ds) @expected-last-modified))
              (is (not= (:store @dsb) recs))
              (is (not= (:last-modified @dsb) @expected-last-modified))
              ;; Refresh ds from storage
              (read-records local-storage ds)
              (is (= (:store @ds) recs))
              (is (= (:last-modified @ds) @expected-last-modified)))
            )
          ))))
)
