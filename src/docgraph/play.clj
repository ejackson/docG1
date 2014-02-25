(ns docgraph.play
  (:use [cascalog.api]
        [clojure.string :as str]
        [cascalog.logic.ops :as o]
        [cascalog.more-taps :only (lfs-delimited)]))

;; ------------------------= Setup ------------------------------
;; File defining the nodes in the graph

;; The full taxonomy has a free text column with commas in it, so I used
;;    csvtool col 1-5 nucc_taxonomy_140.csv -u "|" > nucc_taxonomy.csv
;; to replace the file separator with |
(def taxonomy-file "./resources/taxonomy.csv")

;; Produced by grepping the main npi file for Nashville.
;;    grep -i "NASHVILLE" npidata.csv > nashville.csv
;; This is just laziness, can be done in query to speed it up
(def npi-nashville-file "./resources/nashville.csv")

;; Produced by grepping for physical address
;;   grep -i "34\S\S DICKERSON PIKE" nashville.csv > skyline.csv
(def npi-skyline-file "resources/skyline.csv")

(def teaming-file "./resources/teaming.csv")

;; Write the output of a query to .csv
(defn sink-to-csv [out query]
  (?- (lfs-delimited out :delimiter ",")
      query))


;; ------------------------= Taxonomy =------------------------------
(defn get-taxonomy
  "Process and return tuples from a NUCC taxonomy file"
  [nodes-file]
  (<- [?tax-id ?tax-type ?tax-class ?tax-special]
      ((lfs-delimited nodes-file
                      :delimiter    "|"
                      :skip-header? true)
       :#> 5 {0 ?tax-id
              1 ?tax-type
              2 ?tax-class
              3 ?tax-special})))


;; ------------------------= NPI Data =------------------------------
(defn get-npi
  "Process and return tuples from a NPI file"
  [nodes-file]
  (<- [?doc-id ?doc-type ?doc-locl1 ?doc-city ?doc-state ?doc-zip]
      ((lfs-delimited nodes-file :delimiter ",")
       :#> 329 {0  ?doc-id
                28 ?doc-locl1
                30 ?doc-city
                31 ?doc-state
                32 ?doc-zip
                47 ?doc-type})))

;; Just some munging
(defn doc-ids
  "Returns a set of all the doctor id's returned from the query"
  [query]
  (->> (select-fields query ["?doc-id"]) ??- flatten (into #{})))


(defn doc-map
  "Returns a set of all the doctor id's returned from the query"
  [query]
  (->> query ??- first (reduce (fn [m x] (merge m {(first x) x})) {})))

;; Cache the doc-map
(def all-doc-map
  (delay
   (doc-map (get-npi npi-nashville-file))))

(defmapfn lookup-doc-map
  [doc-id]
  (@all-doc-map doc-id))

;; ------------------------= NPI Data =------------------------------
(defn get-references [edges-file]
  (<- [?ref-from ?ref-to ?ref-count]
      ((lfs-delimited edges-file :delimiter ",") ?ref-from ?ref-to ?ref-count)))

(comment
  ;; Lets join like its 1699
  (sink-to-csv "./resources/out"
   (let [nash-docs (get-npi   npi-nashville-file)
         sky-docs  (get-npi   npi-skyline-file)
         tax  (get-taxonomy   taxonomy-file)
         refs (get-references teaming-file)]

     (<- [?from-doc-id ?from-tax-class ?from-tax-special
          ?to-doc-id   ?to-tax-class   ?to-tax-special]
         ;; Get the references, note the from- and to-docs fields
         (refs ?from-doc-id ?to-doc-id ?ref-count)

         ;; From doc
         (nash-docs ?from-doc-id ?from-doc-type ?from-doc-locl1 ?from-doc-city ?from-doc-state ?from-doc-zip)
         (tax  ?from-doc-type ?from-tax-type ?from-tax-class ?from-tax-special)

         ;; To doc
         (nash-docs ?to-doc-id ?to-doc-type ?to-doc-locl1 ?to-doc-city ?to-doc-state ?to-doc-zip)
         (tax  ?to-doc-type ?to-tax-type ?to-tax-class ?to-tax-special)

         )))


  ;; All references into Skyline, try to avoid joining by cache+filter
  (def sky-doc-ids
    (delay
     (doc-ids (get-npi npi-skyline-file))))

  (defn sky-doc? [doc-id]
    (@sky-doc-ids doc-id))

  (defn is-sky-doc? [doc-id]
    (if (@sky-doc-ids doc-id)
      :sky-doc
      :not-sky-doc))

  (def nash-docs
    (delay
     (get-npi npi-nashville-file)))


  (def nash-doc-ids
    (delay
     (doc-ids @nash-docs)))

  (defn nash-doc? [doc-id]
    (@nash-doc-ids doc-id))

  (defmapcatfn enrich-doc-id [doc-id]
    )

  (def refs
    (get-references teaming-file))

  ;; Get the reference TO nashville doctors
  (sink-to-csv "./resources/out"
               (<- [?from-doc-id ?to-doc-id ?ref-count]
                   (refs ?from-doc-id ?to-doc-id ?ref-count)
                   (nash-doc? ?to-doc-id)))

  ;; No read those references and see which are to skyline onlyg
  (sink-to-csv "./resources/out"
   (<- [?ref-doc ?sky-doc]
       ((get-references "./resources/nash-refs.csv") ?ref-doc ?sky-doc ?ref-count)
       (sky-doc? ?sky-doc)
       ))


  ;; all ref from doc who have ref'd to skyline
  (sink-to-csv "./resources/out"
               (let [ref-to-sky (<- [?ref-doc]
                                    ((get-references "./resources/nash-refs.csv") ?ref-doc ?sky-doc ?ref-count)
                                    (sky-doc? ?sky-doc)
                                    (:distinct true))]
                 (<- [?from-doc ?to-doc ?ref-count]
                     (ref-to-sky ?from-doc)
                     ((get-references "./resources/nash-refs.csv") ?from-doc ?to-doc ?ref-count))))


  ;; all ref from doc who have ref'd to skyline
  ;; bind in referral data
  (sink-to-csv "./resources/out"
               (let [ref-to-sky (<- [?ref-doc]
                                    ((get-references "./resources/nash-refs.csv") ?ref-doc ?sky-doc ?ref-count)
                                    (sky-doc? ?sky-doc)
                                    (:distinct true))
                     tax       (get-taxonomy   taxonomy-file)
                     nash-docs (get-npi npi-nashville-file)]
                 (<- [?from-doc  ?froml1 ?from-tax-type ?from-tax-class ?from-tax-special ?from-sky?
                      ?to-doc ?tol1 ?to-tax-type ?to-tax-class ?to-tax-special ?to-sky?
                      ?ref-count]
                     (ref-to-sky ?from-doc)
                     ((get-references "./resources/nash-refs.csv") ?from-doc ?to-doc ?ref-count)

                     ;; Mark whetehre the ref is *to* a skyline doctor
                     (is-sky-doc? ?to-doc   :> ?to-sky?)
                     (is-sky-doc? ?from-doc :> ?from-sky?)
                     #_(flux-type ?from-sky? ?to-sky? :> ?flux-type)

                     ;; Dig into the from-doc
                     (nash-doc? ?from-doc)
                     (lookup-doc-map ?from-doc :> ?from_id ?from-type ?froml1 ?from-city ?from-state ?from-zip)
                     (tax  ?from-type ?from-tax-type ?from-tax-class ?from-tax-special)

                     ;; Dig into the to-doc
                     (lookup-doc-map ?to-doc :> ?to_id ?to-type ?tol1 ?to-city ?to-state ?to-zip)
                     (tax  ?to-type ?to-tax-type ?to-tax-class ?to-tax-special)

                     )))


  (comment

    ;; Read taxonomy file
    (??-
     (get-taxonomy taxonomy-file))

    ;; Read node file
    (??-
     (get-npi npi-skyline-file))

    ;; Read references file
    (??-
     (get-references teaming-file))


    (??-
     (<- [?doc-type ?type-count]
         ((select-fields
           (get-nodes skyline-file)
           ["?doc-type"])
          ?doc-type)
         (o/count ?type-count))
     )

    ))
