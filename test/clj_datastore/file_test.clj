(ns clj-datastore.file-test
  (use clj-datastore.file))

(def test-file-name "__test_data_store_")
(def test-file-name-full (str test-file-name ".edn"))
(defn delete-test-file []
  (io/delete-file test-file-name-full true))

(defn prepare-test-file []
  (delete-test-file)
  )
(defn test-fixture [f]
  (prepare-test-file)
  (f)
  (delete-test-file)
  )

(use-fixtures :each test-fixture)

(deftest test-local-storage
  (testing "concurrent writes"
    (let [ds (atom (clj_datastore.file.DataStore. [:foo] test-file-name local-storage nil nil nil))
          towrite (atom (replace-store ds (apply conj (concat [(:store @dsa)] [{:foo 5}]))))] ;; Copied from update-and-write-records
      (do-list-records dsa) ;; loads the records
      (try 
        (-> (:storage @towrite) ;; Copied from update-and-write-records
            (write-records towrite))
        (is false "Expected RetryWrite exception")
        (catch clojure.lang.ExceptionInfo e
          (is (= :retry-write (-> (ex-data e)
                                  :cause)))
