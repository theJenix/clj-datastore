(ns clj-datastore.local-storage
  (:require [clj-datastore.file.local-storage :refer [StorageService]]))

(defn get-current-datetime []
  "Gets the current datetime.  This should remain it's own method, so we can inject test times with with-redefs"
  (java.util.Date.))

(defn- retry [retries f & args]
  (loop [retries retries]
    (let [retry (atom false)
          ret (try
                (apply f args)
              (catch Throwable e
                (log/debug "Caught exception!")
                (do-random-wait 10)
                (if (< 0 retries)
                  (do
                    (log/debug "Retrying, " retries "times left")
                    (reset! retry true))
                  (do
                    (log/fatal e)
                    (throw e)))))]
      (if @retry
        (recur (dec retries))
        ret))))

; TODO watch for file changes in a separate process
(def local-storage
  "A generic local file storage mechanism.  Because this relies on the local filesystem, it is susceptable to latencies and other quirks
   (e.g. when flushing the data after spit, the file may be empty or incomplete on the next read).  Care has been taken to mitigate
  these issues, but you should test in your environment before deploying to production."
  (let [build-filename (fn [ds] (-> (:nspace @ds)
                                    (str ".edn")))]
    ;; NOTE: https://bugs.openjdk.java.net/browse/JDK-8177809 means we need to keep our own last-modified time....frowny face
    (reify
      StorageService
      (get-last-modified-date [_ ds]
        (let [fname (build-filename ds)
              ;; Retry a bunch...this mitigates an issue where a read immediately after another thread writes will result in
              ;; a zero length file (which causes an exception in read-string)
              {:keys [_ last-modified]} (retry 50 read-file fname)]
          (when last-modified
            (java.util.Date. last-modified))))
      (read-records [this ds]
        (let [fname (build-filename ds)
              ;; Retry a bunch...this mitigates an issue where a read immediately after another thread writes will result in
              ;; a zero length file (which causes an exception in read-string)
              {:keys [recs last-modified]} (retry 50 read-file fname)]
          (replace-store ds recs nil (java.util.Date. last-modified))))
      (write-records [this ds]
        (locking ds
          (let [fname (build-filename ds)
                recs  (:store @ds)
                now (get-current-datetime)]
            ;; When the last modified date of the data store is not equal to
            ;; that of the underlying file, something's out of sync; in this case
            ;; throw an exception to let the caller figure it out
            ;; Note: this should only happen if two or more write processes are
            ;; sitting at the locking statement (because the calling code should manage this for us otherwise)
            ;; Further note: really, the case we're worried about is when the last-modified[ds] < last-modified[file]
            ;; becasue thats the case that will overwrite someone elses update.  Since last-modified[ds] > last-modified[file]
            ;; should not be possible, we want to throw in this case as well to let the calling code get things straight.
            (when (not= (:last-modified @ds) (get-last-modified-date this ds))
              (throw (d/make-retry-write-exception)))
            (spit fname {:recs recs :last-modified (.. now getTime)})
            (replace-store ds recs nil now)
            ))))))
