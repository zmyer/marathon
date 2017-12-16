(ns jepsen.basic-app-test
  (:require [clojure.test :refer :all]
            [clojure.string :as str]
            [clojure.tools.cli :refer [parse-opts]]
            [jepsen.core :as core]
            [jepsen.cli :as cli]
            [jepsen.checker :as checker]
            [jepsen.checker.marathon-checker :refer :all]
            [jepsen.client.marathon-client :refer :all]
            [jepsen.db :as db]
            [jepsen.generator :as gen]
            [jepsen.generators.marathon-generators :refer :all]
            [jepsen.marathon :refer :all]
            [jepsen.marathon-utils :refer :all]
            [jepsen.nemesis :as nemesis]
            [jepsen.os.debian :as debian]
            [jepsen.os.ubuntu :as ubuntu]
            [jepsen.tests :as tests]))

(deftest basic-app-test
  (is (:valid? (:results (core/run! (merge tests/noop-test
                                           (merge

                                            {:nodes (read-nodes-file "nodes_list")
                                             :ssh {:username "ubuntu",
                                                   :password "ubuntu",
                                                   :strict-host-key-checking false,
                                                   :private-key-path nil}}

                                            {:name      "marathon"
                                             :os        ubuntu/os
                                             :db        (db "1.3.0" "zookeeper-version")}

                                            {:client    (->Client nil)
                                             :generator (track-check-added-apps
                                                         (gen/phases
                                                          (->> (add-app)
                                                               (gen/stagger 10)
                                                               (gen/nemesis
                                                                (gen/seq (cycle [(gen/sleep 40)
                                                                                 {:type :info, :f :start}
                                                                                 (gen/sleep 20)
                                                                                 {:type :info, :f :stop}])))
                                                               (gen/time-limit 100))
                                                          (gen/nemesis (gen/once {:type :info, :f :stop}))
                                                          (gen/once (gen/sleep 30))
                                                          (gen/log "Done generating and launching apps.")))
                                             :nemesis   (nemesis/partition-random-halves)
                                             :checker   (checker/compose
                                                         {:marathon (marathon-app-checker)})})))))))
