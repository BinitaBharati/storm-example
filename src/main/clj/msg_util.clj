(ns msg-util
  (:require [util :as util]))
  
  (defn get-example-str-msg [input-obj]
    (pr-str (bean input-obj)))
  
    (defn get-example-str-msg1 [input]
    (pr-str input))
    
    (defn get-msg-attr [input-str attr]
    (let [msg-attr-map (read-string input-str)]
      ;(println "get-event-attr: event-attr-map = " event-attr-map)
      (get msg-attr-map attr)))
    
    (defn read-str-to-map [input-str]
    (read-string input-str))