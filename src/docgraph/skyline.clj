;; Just look at Skyline to get an idea
(ns docgraph.skyline
  (:use [cascalog.api]
        [clojure.string :as str]
        [cascalog.more-taps :only (lfs-delimited)]))


;; ------------------------= Setup ------------------------------
;; File defining the nodes in the graph
(def nodes-file "./resources/Nashville-head.csv")   ;; **HEAD**
(def edges-file "./resources/teaming.csv")
(def skyline-file "resources/Skyline.csv")          ;; Produced by grep


;; ------------------------= Nodes =------------------------------
(defn nodes-query
  "Returns a query for all the Nashville doctors"
  [nodes-file]
  (<- [?doc-id !org-name ?doc-type]
      ((lfs-delimited nodes-file :delimiter ",") :#> 329 {0  ?doc-id
                                                          4  !org-name
                                                          47 ?doc-type})))

;; Is this sensible ?
;; Just some munging
(defn doc-ids
  "Returns a set of all the doctor id's returned from the query"
  [query]
  (->> query ??- first (map first) (into #{})))

;; ---------------------= Specific Nodes =-----------------------------
;; Grab only Skyline associated doctors

;; Produce delay objects that parse the file the first time they are
;; invoked
(def nashville-docs
  (delay
   (doc-ids (nodes-query nodes-file))))

;; This is the ~200 I fixed to Skyline by Dickerson Pike
(def skyline-docs
  (delay
   (doc-ids (nodes-query skyline-file))))

(def skyref-docs
  (doc-ids
   (<- [?ref-doc]
       ((skyline-referrals edges-file)  ?ref-doc _ _))))


;; Wrap the delay data into a file with known handle for Cascalog to
;; distribute.  This is a bit of admin
(defn nashville-doc? [doc _]
  (@nashville-docs doc))

(defn to-skyline-doc? [from to]
  (@skyline-docs to))

;; The obove is repetitive so should be victim to a macro.

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

(declare skyref-docs)
(defn from-skyref-doc? [from to]
  (skyref-docs from))


;;; Basic queries
(def nashville-referrals (reference-q #'nashville-doc?))
(def hca-referrals       (reference-q #'to-hca-doc?))
(def skyline-referrals   (reference-q #'to-skyline-doc?))
(def skyline-referrers   (reference-q #'to-skyline-doc?))

;;; Now create a query for all referrals from docs who have referred to HCA docs

(comment
  ;; What do I want to see ?  Every reference that touches Skyline.  So
  ;; Every reference where either to or from is skyline, its immediate
  ;; neighborhood


  ;; Cache this
  ;; Run the referral queries
  (execute-to-csv
   (nashville-referrals edges-file) "resources/tmp")

  (execute-to-csv
   (skyline-referrals   edges-file) "resources/tmp2")

  (execute-to-csv
   (all-hca-referring-doc-referrals edges-file)
   "resources/tmp3")

  (<- [?ref-doc]
      ((skyline-referrals edges-file)  ?ref-doc _ _))

  (execute-to-csv
   (skyline-referrers edges-file) "resources/tmp2")

  (skyref-docs "1396882205")


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
        referrer-q))))
