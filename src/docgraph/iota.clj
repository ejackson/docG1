 (ns docgraph.iota
  "Give the word count for a text file using iota and reducers"
  (:require [iota                  :as io]
            [clojure.core.reducers :as r]
            [clojure.string        :as str]
            [clojure.set           :as set]
            [docgraph.header       :as hdr]))

;; -----------------------------------------------------------------------------
;;  Pull the node file out into its schema

;; Turn a single line of the file into a map
(defn line-map [l]
  (->> (str/split l #",")
       (map (fn [s] (str/replace s #"\"" "")))
       (zipmap hdr/file-header)))

(defn keyed [{npi :NPI :as m}]
  {(keyword npi) m})

;; Turn a seq of maps into a map, keyed by the :NPI key of each seq element
(defn file-map
  ([] {})
  ([m1 line-map]
     (assoc m1 (keyword (:NPI line-map)) line-map)))


;; -----------------------------------------------------------------------------
;;  Main file processing
(defn npi-map
  "Returns a map of the word counts"
  [filename]
  (->> (iota/seq filename)
       (r/map line-map)
       (r/map keyed)
       (r/fold merge)))

(defn conjj
  ([] [])
  ([a b] (concat a b)))

(defn npi-vec
  "Returns a map of the word counts"
  [filename]
  (->> (iota/seq filename)
       (r/map line-map)
       (r/fold conjj conj)))


(defn skyline-street? [addr]
  (re-find #"34\S\S DICKERSON PIKE" addr))

(defn hca-doc? [{addr :Provider_First_Line_Business_Practice_Location_Address :as m}]
  (skyline-street? addr))

;; Filter only HCA docs
(defn npi-vec
  "Returns a map of the word counts"
  [filename]
  (->> (iota/seq filename)
       (r/map line-map)
       (r/filter hca-doc?)
       (r/fold conjj conj)))

(defn conjs
  ([] #{})
  ([a b] (set/union a b)))


(defn npi-set
  "Returns a map of the word counts"
  [filename]
  (->> (iota/seq filename)
       (r/map line-map)
       (r/filter hca-doc?)
       (r/map :NPI)
       (r/fold conjs conj)))

(defn hca-line-map [l]
  (->> (str/split l #",")
       (map (fn [s] (str/replace s #"\"" "")))
       (zipmap [:from :to :c])))

(defn hca-referral? [hca-npis {t :to}]
  (hca-npis t))

(defn hca-referring-docs
  "Returns all the referrals to HCA doctors (goes to RAM)"
  [filename hca-referral?]
  (->> (iota/seq filename)
       (r/map hca-line-map)
       (r/filter hca-referral?)
       (r/fold conjj conj)))

;; ------------------------= Setup ------------------------------
;; File defining the nodes in the graph
(def nodes-file "./resources/nashville.csv")   ;; **HEAD**


;; ------------------------= Setup ------------------------------
(comment

  ;; Create a map of the goodies
  (def a
    (npi-map nodes-file))

  ;; Create a list of the goodies
  (def b
    (npi-vec nodes-file))

  ;; Create a set of the goodies
  (def c
    (npi-set nodes-file))


  ;; Get all the docs who refer to HCA
  (def r (hca-referring-docs "./resources/teaming.csv"
                             (partial hca-referral? c)))

  ;; Now get all the referrals from HCA or HCA referring docs
  (def r2 (hca-referring-docs "./resources/teaming.csv"
                              (partial hca-referral? (set/union c
                                                                (into #{} (map :from r))))))

  (count r2)
  (first r2)

  ;; Play with real stuff, then hit the reducers
  (with-open [r (clojure.java.io/reader nodes-file)]
    (doall (->> (line-seq r)
                (map line-map )
                second)))
  )
