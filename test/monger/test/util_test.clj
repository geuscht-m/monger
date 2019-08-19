(ns monger.test.util-test
  (:import com.mongodb.DBObject
           org.bson.Document)
  (:require [monger util conversion]
            [clojure.test :refer :all]))


(deftest get-object-id
  (let [clj-map   { :_id (monger.util/object-id) }
        db-object ^DBObject (monger.conversion/to-db-object clj-map)
        bson-doc  ^Document (monger.conversion/to-bson-document clj-map)
        _id       (:_id clj-map)]
    (is (= _id (monger.util/get-id clj-map)))
    (is (= _id (monger.util/get-id db-object)))
    (is (= _id (monger.util/get-id bson-doc)))))


