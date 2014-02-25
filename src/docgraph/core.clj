(ns docgraph.core
  (:use [cascalog.api]
        [clojure.string :as str]
        [cascalog.more-taps :only (lfs-delimited)]))


;; ------------------------= Setup ------------------------------
;; File defining the nodes in the graph
(def nodes-file "./resources/Nashville.csv")
(def edges-file "./resources/teaming.csv")

;; ------------------------= Nodes =------------------------------
(defn nodes-query
  "Returns a query for all the Nashville doctors"
  [nodes-file]
  (<- [?doc-id !org-name ]
      ((lfs-delimited nodes-file :delimiter ",") :#> 329 {0 ?doc-id 4 !org-name})))

;; ---------------------= HCA Nodes =-----------------------------
;; Grab only HCA associated doctors
;; General predicate for finding HCA docs based on their org name
;; This obviously **COMPLETELY WRONG**, but we can start here
(defn is-hca? [org-name]
  (or (re-find #"HCA " org-name)
      (re-find #"CENTENNIAL" org-name)
      (re-find #"SKYLINE" org-name)
      (re-find #"SOUTHERN HILLS" org-name)))

(defn skyline-zip? [zipcode]
  (re-find #"^37207" zipcode))

(defn skyline-street? [addr]
  (re-find #"34\S\S DICKERSON PIKE" addr))

(defn hca-query
  "Returns a query for all HCA employed doctors"
  [all-nodes-q]
  (<- [?doc-id ?org-name]
      (all-nodes-q ?doc-id ?org-name)
      (is-hca? ?org-name)))

;; Just some munging
(defn doc-ids
  "Returns a set of all the doctor id's returned from the query"
  [query]
  (->> query ??- first (map first) (into #{})))

;; ------------------------= Edges ------------------------------
(defn reference-q [filter-in]
  (fn [edges-file]
    (<- [?from ?to ?count]
        ((lfs-delimited edges-file :delimiter ",") ?from ?to ?count)
        (filter-in ?from ?to))))

(defn execute-to-csv [query out]
  (?- (lfs-delimited out :delimiter ",")
      query))

;; Predicates: these needs a global name for Cascalog to distribute them
(declare nashville-docs)      ;; Will bind at run
(defn nashville-doc? [doc _]
  (nashville-docs doc))

(declare hca-docs)
(defn to-hca-doc? [from to]
  (hca-docs to))

(declare skyline-docs)
(defn to-skyline-doc? [from to]
  (skyline-docs to))


;;; Basic queries
(def nashville-referrals (reference-q #'nashville-doc?))
(def hca-referrals       (reference-q #'to-hca-doc?))
(def skyline-referrals   (reference-q #'to-skyline-doc?))

;;; Now create a query for all referrals from docs who have referred to HCA docs
(defn all-hca-referring-doc-referrals [edges-file]
  (<- [?ref-doc ?any-doc ?any-count !hca-ref]
      ((hca-referrals edges-file)       ?ref-doc ?hca-doc ?hca-count)
      ((nashville-referrals edges-file) ?ref-doc ?any-doc ?any-count)
      (hca-doc? ?any-doc :> !hca-ref)))

(defn all-hca-referring-doc-referrals [edges-file]
  (<- [?ref-doc ?any-doc ?any-count]
      ((hca-referrals edges-file)       ?ref-doc ?hca-doc ?hca-count)
      ((nashville-referrals edges-file) ?ref-doc ?any-doc ?any-count)))

(comment

  ;; Cache this
  (def nashville-docs
    (doc-ids (nodes-query nodes-file)))

  ;; Only 49 on account of poor HCA predicate.... *DONT USE IZ BROKEN*
  (def hca-docs
    (doc-ids (hca-query (nodes-query nodes-file))))

  ;; This is the ~200 I fixed to Skyline by Dickerson Pike
  (def skyline-docs
    (doc-ids (nodes-query "resources/Skyline.csv")))

  ;; Run the referral queries
  (execute-to-csv
   (nashville-referrals edges-file) "resources/tmp")

  (execute-to-csv
   (skyline-referrals      edges-file) "resources/tmp2")

  (execute-to-csv
   (all-hca-referring-doc-referrals edges-file)
   "resources/tmp3")


  ;;; ---------------
  ;;; Cold storage

  (defn nashville-referrals [edges-file out]
  (let [referrer-q (<- [?from ?to ?count]
                       ((lfs-delimited edges-file :delimiter ",") ?from ?to ?count)
                       (fil ?from))]
    (?- (lfs-delimited out :delimiter ",")
        referrer-q)))

  (hca-docs "1457448557")





;; helper
(defn hca-doc? [doc]
  (hca-docs doc))

(defn to-hca-referrals [edges-file out]
  (let [referrer-q (<- [?from ?to ?count]
                       ((lfs-delimited edges-file :delimiter ",") ?from ?to ?count)
                       (hca-doc?))]
    (?- (lfs-delimited out :delimiter ",")
        referrer-q)))


)
