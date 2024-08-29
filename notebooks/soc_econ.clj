^{:kindly/hide-code true}
(ns soc-econ
  (:require [tablecloth.api :as tc]
            [geo.jts :as jts]
            [charred.api :as charred]
            [geo.io :as geoio]
            [tech.v3.tensor :as tensor]
            [tech.v3.datatype.functional :as fun]
            [clojure.java.io :as io]
            [scicloj.kindly.v4.kind :as kind]))




^{:kindly/hide-code true}
(def ramat-gan
  (-> "data/soc-econ/ramat-gan.csv"
      (tc/dataset {:key-fn keyword})))

^{:kindly/hide-code true}
(defn only-one [vs]
  (assert (-> vs count (= 1)))
  (first vs))

^{:kindly/hide-code true}
(def stat-area->info
  (-> ramat-gan
      (tc/group-by :CODE-OF-STATISTICAL-AREA
                   {:result-type :as-map})
      (update-vals (fn [ds]
                     (-> ds
                         (tc/rows :as-maps)
                         only-one)))))



;; ## אשכול חברתי כלכלי 2019

;; ### רמת גן


^{:kindly/hide-code true}
(delay
  (let [enriched-features
        (->> index/stat2011-features-for-drawing
             (filter #(-> % :properties :SHEM_YISHU (= "רמת גן")))
             (map (fn [feature]
                    (when-let [{:keys [CLUSTER-2019]} (-> feature
                                                          :properties
                                                          :STAT11
                                                          stat-area->info)]
                      (let [ratio (* 0.7 (/ CLUSTER-2019
                                            10.0))]
                        (-> feature
                            (assoc :type "Feature")
                            (assoc-in [:properties :center]
                                      (-> feature
                                          :geometry
                                          jts/centroid
                                          index/point->yx))
                            (update :geometry
                                    (fn [geometry]
                                      (-> geometry
                                          geoio/to-geojson
                                          (charred/read-json {:key-fn keyword}))))
                            (assoc-in [:properties :style]
                                      {:color      "yellow"
                                       :fillColor  "yellow"
                                       :opacity 1
                                       :weight 3
                                       :fillOpacity ratio})
                            (assoc-in [:properties :tooltip]
                                      (format "<h1><p>אשכול 2019 : %d</p></h1>"
                                              CLUSTER-2019)))))))
             (filter some?)
             vec)
        center (-> enriched-features
                   (->> (mapv (comp :center :properties)))
                   (tensor/reduce-axis fun/mean 0)
                   vec)]
    (kind/hiccup
     [:div
      (index/choropleth-map
       {:provider "Stadia.AlidadeSmoothDark"
        :center center
        :zoom 14
        :enriched-features enriched-features})])))
