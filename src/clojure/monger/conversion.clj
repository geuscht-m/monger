;; This source code is dual-licensed under the Apache License, version
;; 2.0, and the Eclipse Public License, version 1.0.
;;
;; The APL v2.0:
;;
;; ----------------------------------------------------------------------------------
;; Portions of the code are Copyright (c) 2009 Andrew Boekhoff
;; Copyright (c) 2011-2018 Michael S. Klishin, Alex Petrov, and the ClojureWerkz Team
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
;; Portions of the code are Copyright (c) 2009 Andrew Boekhoff
;; Copyright (c) 2011-2018 Michael S. Klishin, Alex Petrov, and the ClojureWerkz Team.
;; All rights reserved.
;;
;; This program and the accompanying materials are made available under the terms of
;; the Eclipse Public License Version 1.0,
;; which accompanies this distribution and is available at
;; http://www.eclipse.org/legal/epl-v10.html.
;; ----------------------------------------------------------------------------------

(ns monger.conversion
  "Provides functions that convert between MongoDB Java driver classes (DBObject, DBList) and Clojure
   data structures (maps, collections). Most of the time, application developers won't need to use these
   functions directly because Monger Query DSL and many other functions convert documents to Clojure sequences and
   maps automatically. However, this namespace is part of the public API and guaranteed to be stable between minor releases.

   Related documentation guides:

   * http://clojuremongodb.info/articles/inserting.html
   * http://clojuremongodb.info/articles/querying.html"
  (:import [com.mongodb DBObject BasicDBObject BasicDBList]
           [com.mongodb.client MongoCursor]
           [clojure.lang IPersistentMap Named Keyword Ratio]
           [java.util List Map Date Set]
           org.bson.Document
           org.bson.types.ObjectId
           (org.bson.types Decimal128)))

(defprotocol ConvertToDBObject
  (^com.mongodb.DBObject to-db-object [input] "Converts given piece of Clojure data to BasicDBObject MongoDB Java driver uses"))

(extend-protocol ConvertToDBObject
  nil
  (to-db-object [input]
    nil)

  String
  (to-db-object [^String input]
    input)

  Boolean
  (to-db-object [^Boolean input]
    input)

  java.util.Date
  (to-db-object [^java.util.Date input]
    input)

  Ratio
  (to-db-object [^Ratio input]
    (double input))

  Keyword
  (to-db-object [^Keyword input] (.getName input))

  Named
  (to-db-object [^Named input] (.getName input))

  IPersistentMap
  (to-db-object [^IPersistentMap input]
    (let [o (BasicDBObject.)]
      (doseq [[k v] input]
        (.put o (to-db-object k) (to-db-object v)))
      o))

  List
  (to-db-object [^List input] (map to-db-object input))

  Set
  (to-db-object [^Set input] (map to-db-object input))

  DBObject
  (to-db-object [^DBObject input] input)

  com.mongodb.DBRef
  (to-db-object [^com.mongodb.DBRef dbref]
    dbref)

  Object
  (to-db-object [input]
    input))

(defprotocol ConvertToBSONDocument
  (^org.bson.Document to-bson-document [input] "Converts given piece of Clojure data to the BSON Document the MongoDB Java driver uses"))

(extend-protocol ConvertToBSONDocument
  nil
  (to-bson-document [input]
    nil)

  String
  (to-bson-document [^String input]
    input)

  Boolean
  (to-bson-document [^Boolean input]
    input)

  java.util.Date
  (to-bson-document [^java.util.Date input]
    input)

  Ratio
  (to-bson-document [^Ratio input]
    (double input))

  Keyword
  (to-bson-document [^Keyword input] (.getName input))

  Named
  (to-bson-document [^Named input] (.getName input))

  IPersistentMap
  (to-bson-document [^IPersistentMap input]
    (let [o (Document.)]
      (doseq [[k v] input]
        (.put o (to-bson-document k) (to-bson-document v)))
      o))

  List
  (to-bson-document [^List input] (map to-bson-document input))

  Set
  (to-bson-document [^Set input] (map to-bson-document input))

  Document
  (to-bson-document [^Document input] input)

  com.mongodb.DBRef
  (to-bson-document [^com.mongodb.DBRef dbref]
    dbref)

  Object
  (to-bson-document [input]
    input))


(defprotocol ConvertFromDBObject
  (from-db-object [input keywordize] "Converts given DBObject instance to a piece of Clojure data"))

(extend-protocol ConvertFromDBObject
  nil
  (from-db-object [input keywordize] input)

  Object
  (from-db-object [input keywordize] input)

  Decimal128
  (from-db-object [^Decimal128 input keywordize]
    (.bigDecimalValue input)
    )

  List
  (from-db-object [^List input keywordize]
    (vec (map #(from-db-object % keywordize) input)))

  BasicDBList
  (from-db-object [^BasicDBList input keywordize]
    (vec (map #(from-db-object % keywordize) input)))

  com.mongodb.DBRef
  (from-db-object [^com.mongodb.DBRef input keywordize]
    input)

  DBObject
  (from-db-object [^DBObject input keywordize]
    ;; DBObject provides .toMap, but the implementation in
    ;; subclass GridFSFile unhelpfully throws
    ;; UnsupportedOperationException.
    (reduce (if keywordize
              (fn [m ^String k]
                (assoc m (keyword k) (from-db-object (.get input k) true)))
              (fn [m ^String k]
                (assoc m k (from-db-object (.get input k) false))))
            {} (.keySet input))))

(defprotocol ConvertFromBSONDocument
  (from-bson-document [input keywordize] "Converts given BSON Document instance to a piece of Clojure data"))

(extend-protocol ConvertFromBSONDocument
  nil
  (from-bson-document [input keywordize] input)

  Object
  (from-bson-document [input keywordize] input)

  Decimal128
  (from-bson-document [^Decimal128 input keywordize]
    (.bigDecimalValue input)
    )

  List
  (from-bson-document [^List input keywordize]
    (vec (map #(from-db-object % keywordize) input)))

  BasicDBList
  (from-db-object [^BasicDBList input keywordize]
    (vec (map #(from-db-object % keywordize) input)))

  com.mongodb.DBRef
  (from-bson-document [^com.mongodb.DBRef input keywordize]
    input)

  Document
  (from-bson-document [^Document input keywordize]
    ;; DBObject provides .toMap, but the implementation in
    ;; subclass GridFSFile unhelpfully throws
    ;; UnsupportedOperationException.
    (reduce (if keywordize
              (fn [m ^String k]
                (assoc m (keyword k) (from-bson-document (.get input k) true)))
              (fn [m ^String k]
                (assoc m k (from-bson-document (.get input k) false))))
            {} (.keySet input))))


(defprotocol ConvertToObjectId
  (^org.bson.types.ObjectId to-object-id [input] "Instantiates ObjectId from input unless the input itself is an ObjectId instance. In that case, returns input as is."))

(extend-protocol ConvertToObjectId
  String
  (to-object-id [^String input]
    (ObjectId. input))

  Date
  (to-object-id [^Date input]
    (ObjectId. input))

  ObjectId
  (to-object-id [^ObjectId input]
    input))



(defprotocol FieldSelector
  (^com.mongodb.DBObject as-field-selector [input] "Converts values to DBObject that can be used to specify a list of document fields (including negation support)"))

(extend-protocol FieldSelector
  DBObject
  (as-field-selector [^DBObject input]
    input)

  List
  (as-field-selector [^List input]
    (to-db-object (zipmap input (repeat 1))))

  Object
  (as-field-selector [input]
    (to-db-object input)))
