(defproject storm-example "0.1.0-SNAPSHOT"
 :description "FIXME: write description"
  :url "http://example.com/FIXME"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :exclusions [com.sun.jdmk/jmxtools
                com.sun.jmx/jmxri]
  :source-paths ["src/main/clj"]
  :java-source-paths ["src/main/java"]
  :dependencies [[org.clojure/clojure "1.5.1"]
                 [org.clojure/tools.logging "0.2.6"]
                 [org.apache.storm/storm-core "0.9.2-incubating"]
                 ;[activemq-all/activemq-all "5.5.1"] ;; Not present in maven central, install in local maven repo
                 [org.apache.activemq/activemq-all "5.5.1"]
                 [org.slf4j/slf4j-log4j12 "1.5.8"]
                 ]
 )
