;;  "Give the word count for a text file using iota and reducers"
;;  play2 is the compojure version of this

(ns docgraph.fullsky
  (:require [iota                  :as io]
            [clojure.core.reducers :as r]
            [clojure.string        :as str]
            [clojure.set           :as set]
            [docgraph.header       :as hdr]))

;; ------------------------= Setup ------------------------------
;; File defining the nodes in the graph
(def nodes-file     "./resources/nashville.csv")   ;; **HEAD**
(def referrals-file "./resources/teaming.csv")
(def taxonomy-file "./resources/taxonomy.csv")

;; -----------------------------------------------------------------------------
;; Helper monoids.  This are needed for the combiner/reducer pattern
;; Build a set
(defn conjs
  ([] #{})
  ([a b] (set/union a b)))

;; Build a list
(defn conjl
  ([] [])
  ([a b] (concat a b)))


;; -----------------------------------------------------------------------------
;;  Process the NPI nodes file.

;;  Pull the node file out into its schema
(defn ->doc-map [l]
  (->> (str/split l #",")
       (map (fn [s] (str/replace s #"\"" "")))
       (zipmap hdr/file-header)))

;; Predicates for discovering docs of interest
(defn skyline-street? [addr]
  (re-find #"34\S\S DICKERSON PIKE" addr))

(defn skyline-doc? [{addr :Provider_First_Line_Business_Practice_Location_Address :as m}]
  (skyline-street? addr))

;; Returns a map, keyed by npi, of all the doc properties
(defn doc-map
  [filename doc-filter]
  (let [keyed (fn [{npi :NPI :as m}] {npi m})]
    (->> (iota/seq filename)
         (r/map ->doc-map)
         (r/filter doc-filter)
         (r/map keyed)
         (r/fold merge))))

;; -----------------------------------------------------------------------------
;;  Process the referrals file

;; Read from the referals schema
(defn ->referal-map [l]
  (->> (str/split l #",")
       (mapv (fn [s] (str/replace s #"[\",\r]" "")))
       (zipmap [:to :from :c])))

;; Close over a set of npis, return an predicate
;; true if either :to or :from is in that set.
(defn either-to-or-from? [npis]
  (fn  [{t :to f :from}]
    (or (npis t) (npis f))))

(defn referral-list
  "Returns a list of referals passing some predicate"
  [filename interesting-referral?]
  (->> (iota/seq filename)
       (r/map ->referal-map)
       (r/filter interesting-referral?)
       (r/fold conjl conj)))


;; -----------------------------------------------------------------------------
;;  Process the NUCC taxonomy file
;; No need for reducers here, so just read it
(defn taxonomies
  [taxonomy-file]
  (with-open [r (clojure.java.io/reader taxonomy-file)]
    (doall (->> (line-seq r)
                (map (fn [s] (str/split s #"\|")))
                (map #(zipmap [:code :type :class :speciality] %))
                (map (fn [{c :code :as m}]  {c m}))
                (apply merge)))))


;; ------------------------= Setup ------------------------------
(comment
  ;; Return a file with all the references touching skyline

  ;; Step 1. Identify the docs of interest by reading the nodes file
  (def skyline-docs (doc-map nodes-file skyline-doc?))
  (def skyline-npis (into #{} (keys skyline-docs)))

  ;; Step 2.  Pull out all referalls where the npi set is on either side
  (def sky-refs
    (referral-list
     referrals-file
     (either-to-or-from? skyline-npis)))

  ;; Step 3.  Read the taxonomies
  (def taxonomy-codes (taxonomies taxonomy-file))

  (first sky-refs)

  (map
   #(get  %)
   )

  (defn doc-annotation [taxonomy fr]
    (let [code (:Healthcare_Provider_Taxonomy_Code_1 fr)]
      [(:NPI fr)
       (:Provider_First_Line_Business_Practice_Location_Address fr)
       (-> fr code)]))

  (doc-annotation taxonomy-codes (first (vals skyline-docs)))

  ;; Freezer
  ;; Play with real stuff, then hit the reducers
  )
