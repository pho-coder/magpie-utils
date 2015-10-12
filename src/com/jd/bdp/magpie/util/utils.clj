(ns com.jd.bdp.magpie.util.utils
  (:import [java.io InputStreamReader IOException]
           [java.sql Timestamp]
           [java.text SimpleDateFormat]
           [java.util UUID]
           [java.lang.management ManagementFactory RuntimeMXBean]
           
           [org.yaml.snakeyaml Yaml]))

(defn wrap-in-runtime
  "Wraps an exception in a RuntimeException if needed" 
  [^Exception e]
  (if (instance? RuntimeException e)
    e
    (RuntimeException. e)))

(defn find-yaml
  "use Yaml parse conf file. must? for throwing Exception without file"
  [filename & must?]
  (let [^Yaml yaml (Yaml.)
        _resources (.. (Thread/currentThread) getContextClassLoader (getResources filename))]
    (if-not (.hasMoreElements _resources)
      (if must?
        (throw (IOException. (str "resource " filename " is not exists!")))
        {})
      (let [resources (loop [ret []]
                        (if (.hasMoreElements _resources)
                          (recur (conj ret (.nextElement _resources)))
                          ret))
            _ (if (> (count resources) 1)
                (throw (IOException. (str "found multiple " filename " !"))))
            parameters (.load yaml (InputStreamReader. (.openStream (first resources))))]
        (apply conj {} parameters)))))

(defn timestamp2datetime
  ([^Long timestamp-long ^String date-format]
     (.format (SimpleDateFormat. date-format) (Timestamp. timestamp-long)))
  ([timestamp-long]
     (timestamp2datetime timestamp-long "yyyy-MM-dd HH:mm:ss")))

(defn exception-cause? [klass ^Throwable t]
  (->> (iterate #(.getCause ^Throwable %) t)
       (take-while identity)
       (some (partial instance? klass))
       boolean))

(defn current-time-millis []
  (System/currentTimeMillis))

(defn uuid []
  (str (UUID/randomUUID)))

(defn get-pid
  "get process id"
  []
  (let [runtime (ManagementFactory/getRuntimeMXBean)
        ;; format: "pid@hostname"
        name (.getName ^RuntimeMXBean runtime)]
    (.substring name 0 (.indexOf name '@'))))

(defn string->bytes
  [^String a-str & {:keys [encode] :or {encode "utf-8"}}]
  (.getBytes a-str encode))

(defn ^String bytes->string
  [bytes & {:keys [encode] :or {encode "utf-8"}}]
  (String. bytes encode))
