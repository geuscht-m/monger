;; This source code is dual-licensed under the Apache License, version
;; 2.0, and the Eclipse Public License, version 1.0.
;;
;; The APL v2.0:
;;
;; ----------------------------------------------------------------------------------
;; Copyright (c) 2011-2018 Michael S. Klishin, Alex Petrov, and the ClojureWerkz Team
;; Copyright (c) 2012 Toby Hede
;; Copyright (c) 2012 Baishampayan Ghose
;;
;; Licensed under the Apache License, Version 2.0 (the "License");
;; you may not use this file except in compliance with the License.
;; You may obtain a copy of the License at
;;
;;     http://www.apache.org/licenses/LICENSE-2.0
;;
;; Unless required by applicable law or agreed to in writing, software
;; distributed under the License is distributed on an "AS IS" BASIS,
;; WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
;; See the License for the specific language governing permissions and
;; limitations under the License.
;; ----------------------------------------------------------------------------------
;;
;; The EPL v1.0:
;;
;; ----------------------------------------------------------------------------------
;; Copyright (c) 2011-2018 Michael S. Klishin, Alex Petrov, and the ClojureWerkz Team.
;; Copyright (c) 2012 Toby Hede
;; Copyright (c) 2012 Baishampayan Ghose
;;
;; All rights reserved.
;;
;; This program and the accompanying materials are made available under the terms of
;; the Eclipse Public License Version 1.0,
;; which accompanies this distribution and is available at
;; http://www.eclipse.org/legal/epl-v10.html.
;; ----------------------------------------------------------------------------------

(ns monger.collection
  "Provides key functionality for interaction with MongoDB: inserting, querying, updating and deleting documents, performing Aggregation Framework
   queries, creating and dropping indexes, creating collections and more.

   For more advanced read queries, see monger.query.

   Related documentation guides:

   * http://clojuremongodb.info/articles/getting_started.html
   * http://clojuremongodb.info/articles/inserting.html
   * http://clojuremongodb.info/articles/querying.html
   * http://clojuremongodb.info/articles/updating.html
   * http://clojuremongodb.info/articles/deleting.html
   * http://clojuremongodb.info/articles/aggregation.html"
  (:refer-clojure :exclude [find remove count drop distinct empty? any? update])
  (:import [com.mongodb Mongo DB DBCollection WriteResult DBObject WriteConcern
            DBCursor MapReduceCommand MapReduceCommand$OutputType AggregationOutput
            AggregationOptions AggregationOptions$OutputMode MongoNamespace]
           [com.mongodb.client MongoDatabase MongoCollection MongoCursor FindIterable]
           [com.mongodb.client.result DeleteResult UpdateResult]
           [com.mongodb.client.model CreateCollectionOptions FindOneAndUpdateOptions FindOneAndDeleteOptions UpdateOptions IndexOptions ReturnDocument]
           [java.util List Map]
           [java.util.concurrent TimeUnit]
           [clojure.lang IPersistentMap ISeq]
           org.bson.types.ObjectId
           [org.bson BsonString Document])
  (:require [monger.core :as mc]
            [monger.result :as mres]
            [monger.conversion :refer :all]
            [monger.constraints :refer :all]
            [monger.util :refer [into-array-list]]
            [clojure.string :as str :refer [starts-with?]]))

;; Helper functions that mask some of the differences between the 2.x and 3.x APIs
(defn- contains-update-operator?
  [^Map document]
  (let [first-key (first (keys document))]
    (or (nil? first-key) (starts-with? (name first-key) "$"))))

(defn- update-syntax-driver-3
  "Very simple check if the document passed in contains an update operator at the top level.
   If it doesn't the function returns the input document wrapped in a $set operation."
  [^Map document]
  (if (contains-update-operator? document)
    document
    {:$set document}))

;;
;; API
;;

;;
;; monger.collection/insert
;;

(defn insert
  "Saves document to collection and returns a write result monger.result/acknowledged?
   and related functions operate on. You can optionally specify a WriteConcern.

   In case you need the exact inserted document returned, with the :_id key generated,
   use monger.collection/insert-and-return instead."
  ;; Check if insertOne is the correct function here
  ([^MongoDatabase db ^String coll document]
     (insert db coll document ^WriteConcern mc/*mongodb-write-concern*))
  ([^MongoDatabase db ^String coll document ^WriteConcern concern]
     (.insertOne (.getCollection db (name coll))
              ^Document (to-bson-document document))))
              ;;concern)))


(defn ^clojure.lang.IPersistentMap insert-and-return
  "Like monger.collection/insert but returns the inserted document as a persistent Clojure map.

  If the :_id key wasn't set on the document, it will be generated and merged into the returned
  map."
  ([^MongoDatabase db ^String coll document]
     (insert-and-return db coll document ^WriteConcern mc/*mongodb-write-concern*))
  ([^MongoDatabase db ^String coll document ^WriteConcern concern]
     ;; MongoDB Java driver will generate the _id and set it but it
     ;; tries to mutate the inserted DBObject and it does not work
     ;; very well in our case, because that DBObject is short lived
     ;; and produced from the Clojure map we are passing in. Plus,
     ;; this approach is very awkward with immutable data structures
     ;; being the default. MK.
     (let [doc (merge {:_id (ObjectId.)} document)]
       (insert db coll doc concern)
       doc)))


;; (defn ^WriteResult insert-batch
;;   "Saves documents do collection. You can optionally specify WriteConcern as a third argument."
;;   ([^MongoDatabase db ^String coll ^List documents]
;;      (insert-batch db coll documents mc/*mongodb-write-concern*))
;;      ;;(.insertMany (.getCollection db (name coll))
;;      ;;         ^List (to-bson-document documents)
;;      ;;         ^WriteConcern mc/*mongodb-write-concern*))
;;   ([^MongoDatabase db ^String coll ^List documents ^WriteConcern concern]
;;      (.insertMany (.getCollection db (name coll))
;;               ^List (to-bson-document documents)
;;               ;;concern)))
;;               )))

(defn insert-batch
  "Saves documents do collection. You can optionally specify WriteConcern as a third argument."
  ([^MongoDatabase db ^String coll ^List documents]
     (insert-batch db coll documents mc/*mongodb-write-concern*))
  ([^MongoDatabase db ^String coll ^List documents ^WriteConcern concern]
     (.insertMany (.getCollection db (name coll))
              ^List (to-bson-document documents)
              ;;concern)))
              )))

;;
;; monger.collection/find
;;

(defn ^MongoCursor find
  "Queries for objects in this collection.
   This function returns a MongoCursor, which allows you to iterate over BSON Documents.
   If you want to manipulate clojure sequences maps, use find-maps."
  ([^MongoDatabase db ^String coll]
     (.iterator (.find (.getCollection db (name coll)))))
  ([^MongoDatabase db ^String coll ^Map ref]
     (.iterator (.find (.getCollection db (name coll))
                                     (to-bson-document ref))))
  ([^MongoDatabase db ^String coll ^Map ref fields]
     (.iterator (.projection (.find (.getCollection db (name coll))
                                    (to-bson-document ref))
                             (as-field-selector fields)))))

(defn find-maps
  "Queries for objects in this collection.
   This function returns clojure Seq of Maps.
   If you want to work directly with DBObject, use find."
  ([^MongoDatabase db ^String coll]
     (with-open [dbc (find db coll)]
       (doall (map (fn [x] (from-bson-document x true)) (iterator-seq dbc)))))
  ([^MongoDatabase db ^String coll ^Map ref]
     (with-open [^MongoCursor dbc (find db coll ref)]
       (doall (map (fn [x] (from-bson-document x true)) (iterator-seq dbc)))))
  ([^MongoDatabase db ^String coll ^Map ref fields]
     (find-maps db coll ref fields true))
  ([^MongoDatabase db ^String coll ^Map ref fields keywordize]
     (with-open [dbc (find db coll ref fields)]
       (doall (map (fn [x] (from-bson-document x keywordize)) (iterator-seq dbc))))))

(defn find-seq
  "Queries for objects in this collection, returns ISeq of Documents."
  ([^MongoDatabase db ^String coll]
     (with-open [dbc (find db coll)]
       (iterator-seq dbc)))
  ([^MongoDatabase db ^String coll ^Map ref]
     (with-open [dbc (find db coll ref)]
       (iterator-seq dbc)))
  ([^MongoDatabase db ^String coll ^Map ref fields]
     (with-open [dbc (find db coll ref fields)]
       (iterator-seq dbc))))

;;
;; monger.collection/find-one
;;

(defn ^Document find-one
  "Returns a single DBObject from this collection matching the query."
  ([^MongoDatabase db ^String coll ^Map ref]
     (first (.find (.getCollection db (name coll))
                   (to-bson-document ref))))
  ([^MongoDatabase db ^String coll ^Map ref fields]
     (first (.projection (.find (.getCollection db (name coll))
                                (to-bson-document ref))
                         (as-field-selector fields)))))

(defn ^IPersistentMap find-one-as-map
  "Returns a single object converted to Map from this collection matching the query."
  ([^MongoDatabase db ^String coll ^Map ref]
     (from-bson-document ^Document (find-one db coll ref) true))
  ([^MongoDatabase db ^String coll ^Map ref fields]
     (from-bson-document ^Document (find-one db coll ref fields) true))
  ([^MongoDatabase db ^String coll ^Map ref fields keywordize]
     (from-bson-document ^Document (find-one db coll ref fields) keywordize)))


;;
;; monger.collection/find-and-modify
;;

(defn ^IPersistentMap find-and-modify
  "Atomically modify a document (at most one) and return it."
  ([^MongoDatabase db ^String coll ^Map conditions ^Map document {:keys [fields sort remove return-new upsert keywordize] :or
                                                       {fields nil
                                                        sort nil
                                                        remove false
                                                        return-new false
                                                        upsert false
                                                        keywordize true}}]
     (let [^MongoCollection mcoll (.getCollection db (name coll))
           maybe-fields (when fields (as-field-selector fields))
           maybe-sort (when sort (to-bson-document sort))
           operation-options (if remove
                               (.sort (FindOneAndDeleteOptions.) maybe-sort)
                               (.projection (.returnDocument (.upsert (.sort (FindOneAndUpdateOptions.) maybe-sort) upsert) (if return-new ReturnDocument/AFTER ReturnDocument/BEFORE)) maybe-fields)) ]
       (print "Conditions: ")(println conditions)
       (print "Document: ")(println document)
       (print "Keys: ")(println upsert)
       (from-bson-document
        (if remove
          (.findOneAndDelete mcoll (to-bson-document conditions) operation-options)
          (.findOneAndUpdate mcoll (to-bson-document conditions) (to-bson-document (update-syntax-driver-3 document)) operation-options)) keywordize)))) ;;maybe-fields

;;
;; monger.collection/find-by-id
;;

(defn ^Document find-by-id
  "Returns a single object with matching _id field."
  ([^MongoDatabase db ^String coll id]
     (check-not-nil! id "id must not be nil")
     (find-one db coll {:_id id}))
  ([^MongoDatabase db ^String coll id fields]
     (check-not-nil! id "id must not be nil")
     (find-one db coll {:_id id} fields)))

(defn ^IPersistentMap find-map-by-id
  "Returns a single object, converted to map with matching _id field."
  ([^MongoDatabase db ^String coll id]
     (check-not-nil! id "id must not be nil")
     (find-one-as-map db coll {:_id id}))
  ([^MongoDatabase db ^String coll id fields]
     (check-not-nil! id "id must not be nil")
     (find-one-as-map db coll {:_id id} fields))
  ([^MongoDatabase db ^String coll id fields keywordize]
     (check-not-nil! id "id must not be nil")
     (find-one-as-map db coll {:_id id} fields keywordize)))

;;
;; monger.collection/count
;;

(defn count
  "Returns the number of documents in this collection.

  Takes optional conditions as an argument."
  (^long [^MongoDatabase db ^String coll]
         (.count (.getCollection db (name coll))))
  (^long [^MongoDatabase db ^String coll ^Map conditions]
         (.count (.getCollection db (name coll)) (to-bson-document conditions))))

(defn any?
  "Whether the collection has any items at all, or items matching query."
  ([^MongoDatabase db ^String coll]
     (> (count db coll) 0))
  ([^MongoDatabase db ^String coll ^Map conditions]
     (> (count db coll conditions) 0)))


(defn empty?
  "Whether the collection is empty."
  [^MongoDatabase db ^String coll]
  (= (count db coll {}) 0))

;; monger.collection/update


(defn ^UpdateResult update
  "Performs an update operation.

  Please note that update is potentially destructive operation. It updates document with the given set
  emptying the fields not mentioned in the new document. In order to only change certain fields, use
  \"$set\".

  You can use all the MongoDB modifier operations ($inc, $set, $unset, $push, $pushAll, $addToSet, $pop, $pull
  $pullAll, $rename, $bit) here as well.

  It also takes options, such as :upsert and :multi.
  By default :upsert and :multi are false."
  ([^MongoDatabase db ^String coll ^Map conditions ^Map document]
     (update db coll conditions document {}))
  ([^MongoDatabase db ^String coll ^Map conditions ^Map document {:keys [upsert multi write-concern]
                                                       :or {upsert false
                                                            multi false
                                                            write-concern mc/*mongodb-write-concern*}}]
   (if multi
     (.updateMany (.getCollection db (name coll))
                  (to-bson-document conditions)
                  (to-bson-document (update-syntax-driver-3 document))
                  (.upsert (UpdateOptions.) upsert)
                  ;;multi
                  ;;write-concern)))
                  )
     (.updateOne (.getCollection db (name coll))
                 (to-bson-document conditions)
                 (to-bson-document (update-syntax-driver-3 document))
                 (.upsert (UpdateOptions.) upsert)))))
   

(defn ^UpdateResult upsert
  "Performs an upsert.

   This is a convenience function that delegates to monger.collection/update and
   sets :upsert to true.

   See monger.collection/update documentation"
  ([^MongoDatabase db ^String coll ^Map conditions ^Map document]
     (upsert db coll conditions document {}))
  ([^MongoDatabase db ^String coll ^Map conditions ^Map document {:keys [multi write-concern]
                                                       :or {multi false
                                                            write-concern mc/*mongodb-write-concern*}}]
     (update db coll conditions document {:multi multi :write-concern write-concern :upsert true})))

(defn ^UpdateResult update-by-id
  "Update a document with given id"
  ([^MongoDatabase db ^String coll id ^Map document]
     (update-by-id db coll id document {}))
  ([^MongoDatabase db ^String coll id ^Map document {:keys [upsert write-concern]
                                          :or {upsert false
                                               write-concern mc/*mongodb-write-concern*}}]
   (let [options (.upsert (UpdateOptions.) upsert)]
     (check-not-nil! id "id must not be nil")
     (.updateOne (.getCollection db (name coll))
                 (to-bson-document {:_id id})
                 (to-bson-document document)
                 options
                 ;;upsert
                 ;;false
                 ;;write-concern)))
                 ))))

(defn ^UpdateResult update-by-ids
  "Update documents by given ids"
  ([^MongoDatabase db ^String coll ids ^Map document]
     (update-by-ids db coll ids document {}))
  ([^MongoDatabase db ^String coll ids ^Map document {:keys [upsert write-concern]
                                           :or {upsert false
                                                write-concern mc/*mongodb-write-concern*}}]
   (let [options (.upsert (UpdateOptions.) upsert)]
     (check-not-nil! (seq ids) "ids must not be nil or empty")
     (.updateMany (.getCollection db (name coll))
                  (to-bson-document {:_id {"$in" ids}})
                  (to-bson-document document)
                  options
                  ;;upsert
                  ;;true
                  ;;write-concern)))
                  ))))


;; monger.collection/save

(defn ^WriteResult save
  "Saves an object to the given collection (does insert or update based on the object _id).

   If the object is not present in the database, insert operation will be performed.
   If the object is already in the database, it will be updated.

   This function returns write result. If you want to get the exact persisted document back,
   use `save-and-return`."
  ([^MongoDatabase db ^String coll ^Map document]
   (save (.getCollection db (name coll)) document))
   ;; (.findOneAndUpdate (.getCollection db (name coll))
   ;;                    ;;(to-bson-document {:_id (get document :_id)})
   ;;                    (to-bson-document (if (contains? document :_id) {:_id (get document :_id)} {}))
   ;;                    (to-bson-document document)
   ;;                    (.upsert (FindOneAndUpdateOptions.) true)))
            ;;mc/*mongodb-write-concern*))
  ([^MongoDatabase db ^String coll ^Map document ^WriteConcern write-concern]
   (save db coll document))  ;; Note - in 3.x you add the write concern by creating a MongoCollection with the appropriate w/c
   ;;(.findOneAndUpdate (.getCollection db (name coll))
   ;;                   (to-bson-document (if (contains? document :_id) {:_id (get document :_id)} {}))
   ;;                   (to-bson-document document)
   ;;                   (.upsert (FindOneAndUpdateOptions.) true))))
   ;;write-concern)))
  ([^MongoCollection coll ^Map document]
   (let [filter-doc (to-bson-document (if (contains? document :_id) {:_id (get document :_id)} {}))
         save-doc (to-bson-document (update-syntax-driver-3 document))]
     (.findOneAndUpdate coll
                        filter-doc
                        save-doc
                        (.upsert (FindOneAndUpdateOptions.) true)))))

   

(defn ^clojure.lang.IPersistentMap save-and-return
  "Saves an object to the given collection (does insert or update based on the object _id).

   If the object is not present in the database, insert operation will be performed.
   If the object is already in the database, it will be updated.

   This function returns the exact persisted document back, including the `:_id` key in
   case of an insert.

   If you want to get write result back, use `save`."
  ([^MongoDatabase db ^String coll ^Map document]
     (save-and-return db coll document ^WriteConcern mc/*mongodb-write-concern*))
  ([^MongoDatabase db ^String coll ^Map document ^WriteConcern write-concern]
     ;; see the comment in insert-and-return. Here we additionally need to make sure to not scrap the :_id key if
     ;; it is already present. MK.
   (let [doc (merge {:_id (ObjectId.)} document)]
     (print "save-and-return document ")
     ;;(println doc)
     ;;(println document)
     (save db coll doc write-concern)
     doc)))


;; monger.collection/remove

(defn ^DeleteResult remove
  "Removes objects from the database."
  ;; Note: Check if there is scope for the use of deleteOne as well
  ([^MongoDatabase db ^String coll]
     (.deleteMany (.getCollection db (name coll)) (to-bson-document {})))
  ([^MongoDatabase db ^String coll ^Map conditions]
     (.deleteMany (.getCollection db (name coll)) (to-bson-document conditions))))


(defn ^DeleteResult remove-by-id
  "Removes a single document with given id"
  [^MongoDatabase db ^String coll id]
  (check-not-nil! id "id must not be nil")
  (let [coll (.getCollection db (name coll))]
    (.deleteOne coll (to-bson-document {:_id id}))))

(defn purge-many
  "Purges (removes all documents from) multiple collections. Intended
   to be used in test environments."
  [^MongoDatabase db xs]
  (doseq [coll xs]
    (remove db coll)))

;;
;; monger.collection/create-index
;;

(defn create-index
  "Forces creation of index on a set of fields, if one does not already exists."
  ([^MongoDatabase db ^String coll ^Map keys]
     (.createIndex (.getCollection db (name coll)) (as-field-selector keys)))
  ([^MongoDatabase db ^String coll ^Map keys ^Map options]
     (.createIndex (.getCollection db (name coll))
                   (as-field-selector keys)
                   (to-bson-document options))))


;;
;; monger.collection/ensure-index
;;

(defn ensure-index
  "Creates an index on a set of fields, if one does not already exist.
   This operation is inexpensive in the case when an index already exists.

   Options are:

   :unique (boolean) to create a unique index
   :name (string) to specify a custom index name and not rely on the generated one"
  ([^MongoDatabase db ^String coll ^Map keys]
     (.createIndex (.getCollection db (name coll)) (as-field-selector keys)))
  ([^MongoDatabase db ^String coll ^Map keys ^Map options]
   (let [create-options (.name (.unique (IndexOptions.) (get options :unique)) (get options :name))] ;; TODO - Add/test support for :name
     (.createIndex (.getCollection db (name coll))
                   (as-field-selector keys)
                   create-options)))
  ([^MongoDatabase db ^String coll ^Map keys ^String index-name unique?]
     (let [index-opts (.unique (.name (IndexOptions.) index-name) unique?)]
       (.createIndex (.getCollection db (name coll))
                     (as-field-selector keys)
                     index-opts))))


;; NOTE: listIndexes returns an iterator, not a list of documents
;;       We have to do some gymnastics to convert it back to a list
;;       and it doesn't look like map or reduce readily accept protocols (need to check)

(defn- bson-converter [^Document input]
  (reduce (fn [m ^String k] (assoc m (keyword k) (from-bson-document (.get input k) true))) {} (.keySet input)))

;;
;; monger.collection/indexes-on
;;

(defn indexes-on
  "Return a list of the indexes for this collection."
  [^MongoDatabase db ^String coll]
  (doall (map bson-converter (iterator-seq (.iterator (.listIndexes (.getCollection db (name coll))))))))


;;
;; monger.collection/drop-index
;;

(defn drop-index
  "Drops an index from this collection."
  [^MongoDatabase db ^String coll idx]
  (if (string? idx)
    (.dropIndex (.getCollection db (name coll)) ^String idx)
    (.dropIndex (.getCollection db (name coll)) (to-bson-document idx))))

(defn drop-indexes
  "Drops all indixes from this collection."
  [^MongoDatabase db ^String coll]
  (.dropIndexes (.getCollection db (name coll))))


;;
;; monger.collection/exists?, /create, /drop, /rename
;;


(defn exists?
  "Checks weather collection with certain name exists."
  ([^MongoDatabase db ^String coll]
   ;;(.collectionExists db coll)))
   (some? (some #(= coll %) (.listCollectionNames db)))))

(defn- buildCreateOpt
  "Takes a map and tries to build CreateCollectionOptions object from it

   Return nil if there are no options"
  [^Map options]
  (if (clojure.core/empty? options)
    nil
    (when (contains? options :capped)
      (.maxDocuments (.sizeInBytes (.capped (CreateCollectionOptions.) (get options :capped)) (get options :size)) (get options :max)))))


(defn create
  "Creates a collection with a given name and options.

   Options are:

   :capped (pass true to create a capped collection)
   :max (number of documents)
   :size (max allowed size of the collection, in bytes)"
  [^MongoDatabase db ^String coll ^Map options]
  (let [createOpt (buildCreateOpt options)]
    (if (nil? createOpt)
      (.createCollection db coll)
      (.createCollection db coll createOpt))
    (.getCollection db coll)))  ;; NOTE: createCollection doesn't return anything, we have to call .getCollection to get a handle on the collection

(defn drop
  "Deletes collection from database."
  [^MongoDatabase db ^String coll]
  (.drop (.getCollection db (name coll))))

(defn rename
  "Renames collection."
  ([^MongoDatabase db ^String from, ^String to]
     (.renameCollection (.getCollection db from) (MongoNamespace. (.getName db) to)))
  ([^MongoDatabase db ^String from ^String to drop-target?]
     (.renameCollection (.getCollection db from) (MongoNamespace. (.getName db) to) drop-target?)))

;;
;; Map/Reduce
;;

(defn map-reduce
  "Performs a map reduce operation"
  ([^MongoDatabase db ^String coll ^String js-mapper ^String js-reducer ^String output ^Map query]
     (let [coll (.getCollection db (name coll))]
       ;;(.mapReduce coll js-mapper js-reducer output (to-bson-document query))))
       (.mapReduce coll js-mapper js-reducer)))
  (;;[^MongoDatabase db ^String coll ^String js-mapper ^String js-reducer ^String output ^MapReduceCommand$OutputType output-type ^Map query]
   [^MongoDatabase db ^String coll ^String js-mapper ^String js-reducer ^String output output-type ^Map query]
     (let [coll (.getCollection db (name coll))]
       ;;(.mapReduce coll js-mapper js-reducer output output-type (to-bson-document query)))))
       (.mapReduce coll js-mapper js-reducer Document)))) ;; TODO: Fix output type setting/map


;;
;; monger.collection/distinct
;;
;; FIXME - currently only works for String return values
(defn distinct
  "Finds distinct values for a key"
  ([^MongoDatabase db ^String coll ^String key]
     (doall (iterator-seq (.iterator (.distinct (.getCollection db (name coll)) ^String (name key) String)))))
  ([^MongoDatabase db ^String coll ^String key ^Map query]
     (doall (iterator-seq (.iterator (.distinct (.getCollection db (name coll)) ^String (name key) (to-bson-document query) String))))))


;;
;; Aggregation
;;

(defn- build-aggregation-options
  ^AggregationOptions
  [{:keys [^Boolean allow-disk-use cursor ^Long max-time]}]
  (cond-> (AggregationOptions/builder)
     allow-disk-use       (.allowDiskUse allow-disk-use)
     cursor               (.outputMode AggregationOptions$OutputMode/CURSOR)
     max-time             (.maxTime max-time TimeUnit/MILLISECONDS)
     (:batch-size cursor) (.batchSize (int (:batch-size cursor)))
     true                 .build))

(defn aggregate
  "Executes an aggregation query. MongoDB 2.2+ only.
   Accepts the options :allow-disk-use and :cursor (a map with the :batch-size
   key), as described in the MongoDB manual. Additionally, the :max-time option
  is supported, for specifying a limit on the execution time of the query in
  milliseconds.

  :keywordize option that control if resulting map keys will be turned into keywords, default is true.

  See http://docs.mongodb.org/manual/applications/aggregation/ to learn more."
  [^MongoDatabase db ^String coll stages & opts]
  (let [coll (.getCollection db coll)
        agg-opts (build-aggregation-options opts)
        pipe (into-array-list (to-bson-document stages))
        ;;res (.aggregate coll pipe agg-opts)
        res (.aggregate coll pipe)   ;; TODO: Check what is necessary for agg-opts
        {:keys [^Boolean keywordize]
         :or            {keywordize true}} opts]
    ;;(map #(from-db-object % keywordize) (iterator-seq res))))
    (map #(from-bson-document % keywordize) res)))

(defn explain-aggregate
  "Returns the explain plan for an aggregation query. MongoDB 2.2+ only.

  See http://docs.mongodb.org/manual/applications/aggregation/ to learn more."
  [^MongoDatabase db ^String coll stages & opts]
  (let [coll (.getCollection db coll)
        agg-opts (build-aggregation-options opts)
        pipe (cons (into-array-list (to-bson-document stages)) (to-bson-document {:explain true}))
        ;;res (.explainAggregate coll pipe agg-opts)]
        ;;res (.aggregate coll pipe)]
        res nil]
    (println pipe)
    (from-bson-document res true)))
;;
;; Misc
;;

(def ^{:const true}
  system-collection-pattern #"^(system|fs)")

(defn system-collection?
  "Evaluates to true if the given collection name refers to a system collection. System collections
   are prefixed with system. or fs. (default GridFS collection prefix)"
  [^String coll]
  (re-find system-collection-pattern coll))
