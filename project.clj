(defproject org.elasticsearch/newsroom "0.99.0"
  :description "Publish/consume"
  :url "https://github.com/drewr/newsroom"
  :license {:name "The Apache 2 License"
            :url "http://www.apache.org/licenses/LICENSE-2.0.html"
            :distribution :repo}
  :dependencies [[slingshot "0.10.3"]
                 [cheshire "5.2.0"]
                 [clj-http "0.7.6"]]
  :profiles {:dev {:dependencies [[org.clojure/clojure "1.5.1"]]}})
