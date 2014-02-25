(defproject docgraph "0.1.0-SNAPSHOT"
  :description "FIXME: write description"
  :url "http://example.com/FIXME"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :dependencies [[org.clojure/clojure "1.5.1"]
                 [cascalog "2.0.0"]
                 [iota     "1.1.1"]]
  :profiles { :dev {:dependencies [[org.apache.hadoop/hadoop-core "1.1.2"]]}}
  :repositories [["conjars.org" "http://conjars.org/repo"]]
  :jvm-opts ["-Xms768m" "-Xmx768m"])
