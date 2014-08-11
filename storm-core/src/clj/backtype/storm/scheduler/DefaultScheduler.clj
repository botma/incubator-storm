;; Licensed to the Apache Software Foundation (ASF) under one
;; or more contributor license agreements.  See the NOTICE file
;; distributed with this work for additional information
;; regarding copyright ownership.  The ASF licenses this file
;; to you under the Apache License, Version 2.0 (the
;; "License"); you may not use this file except in compliance
;; with the License.  You may obtain a copy of the License at
;;
;; http://www.apache.org/licenses/LICENSE-2.0
;;
;; Unless required by applicable law or agreed to in writing, software
;; distributed under the License is distributed on an "AS IS" BASIS,
;; WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
;; See the License for the specific language governing permissions and
;; limitations under the License.
(ns backtype.storm.scheduler.DefaultScheduler
  (:use [backtype.storm util config])
  (:require [backtype.storm.scheduler.EvenScheduler :as EvenScheduler])
  (:import [backtype.storm.scheduler IScheduler Topologies
            Cluster TopologyDetails WorkerSlot SchedulerAssignment
            EvenScheduler ExecutorDetails])
  (:gen-class
    :implements [backtype.storm.scheduler.IScheduler]))

(defn- bad-slots [existing-slots num-executors num-workers]
  (if (= 0 num-workers)
    '()
    (let [distribution (atom (integer-divided num-executors num-workers))
          keepers (atom {})]
      (doseq [[node+port executor-list] existing-slots :let [executor-count (count executor-list)]]
        (when (pos? (get @distribution executor-count 0)) ;;对应workerslot中的execut个数在distribution的除法中有值
          (swap! keepers assoc node+port executor-list)   ;;保留这个workerslot的executor
          (swap! distribution update-in [executor-count] dec) ;;将对应的值减1
          ))
      (->> @keepers
           keys
           (apply dissoc existing-slots)
           keys
           (map (fn [[node port]]
                  (WorkerSlot. node port)))))))


;;正常的supervisor和端口都可以reassign
(defn slots-can-reassign [^Cluster cluster slots]
  (->> slots
      (filter
        (fn [[node port]]
          (if-not (.isBlackListed cluster node)
            (if-let [supervisor (.getSupervisorById cluster node)]
              (.contains (.getAllPorts supervisor) (int port))
              ))))))

(defn -prepare [this conf]
  )

(defn default-schedule [^Topologies topologies ^Cluster cluster]
  (let [needs-scheduling-topologies (.needsSchedulingTopologies cluster topologies)]
    (doseq [^TopologyDetails topology needs-scheduling-topologies
            :let [topology-id (.getId topology)
                  available-slots (->> (.getAvailableSlots cluster)
                                       (map #(vector (.getNodeId %) (.getPort %)))) ;;将workerSlot转换为 [nodeid,port]
                  all-executors (->> topology     ;; 该topology需要的executor
                                     .getExecutors
                                     (map #(vector (.getStartTask %) (.getEndTask %))) ;; 转换为 [start,end]
                                     set)
                  alive-assigned (EvenScheduler/get-alive-assigned-node+port->executors cluster topology-id)
                  alive-executors (->> alive-assigned vals (apply concat) set)
                  can-reassign-slots (slots-can-reassign cluster (keys alive-assigned))
                  total-slots-to-use (min (.getNumWorkers topology)  ;;可用的workerslot
                                          (+ (count can-reassign-slots) (count available-slots)))
                  bad-slots (if (or (> total-slots-to-use (count alive-assigned)) 
                                    (not= alive-executors all-executors))
                                (bad-slots alive-assigned (count all-executors) total-slots-to-use)
                                [])]]
      (.freeSlots cluster bad-slots)
      (EvenScheduler/schedule-topologies-evenly (Topologies. {topology-id topology}) cluster))))

(defn -schedule [this ^Topologies topologies ^Cluster cluster]
  (default-schedule topologies cluster))
