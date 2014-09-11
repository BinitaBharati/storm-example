(ns util
  (:require [clojure.tools.logging :as ctl]))

(defn log-info [ & input] (ctl/info (apply str input)))