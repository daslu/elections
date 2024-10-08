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




;; ## רמת גן
;; ### אשכול חברתי כלכלי 2019

;; ירוק - נתוני למ"ס

;; אדום - אין נתונים

^{:kindly/hide-code true}
(delay
  (let [enriched-features
        (->> index/stat2011-features-for-drawing
             (filter #(-> % :properties :SHEM_YISHU (= "רמת גן")))
             (map (fn [feature]
                    (let [{:keys [CLUSTER-2019]} (-> feature
                                                     :properties
                                                     :STAT11
                                                     stat-area->info)]
                      (let [ratio (if CLUSTER-2019
                                    (* 0.7 (/ CLUSTER-2019
                                              10.0))
                                    0)
                            color (if CLUSTER-2019
                                    "darkgreen"
                                    "darkred")]
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
                                      {:color      color
                                       :fillColor  color
                                       :opacity 1
                                       :weight 3
                                       :fillOpacity ratio})
                            (assoc-in [:properties :tooltip]
                                      (format "<h1><p>%d</p></h1>"
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
       {:provider "Stadia.AlidadeSmooth"
        ;;"OpenStreetMap.Mapnik"
        :center center
        :zoom 14
        :enriched-features enriched-features})])))


;; ### אחוז הצבעה לליכוד 2023







(delay
  (let [enriched-features
        (->> stat2011-features-for-drawing
             (filter #(-> % :properties :SHEM_YISHU (= "רמת גן")))
             (map (fn [feature]
                    (when-let [place-stat2011->aggregation (election->place-stat2011->aggregation
                                                            election)]
                      (when-let [{:as aggregation
                                  :keys [voters
                                         meshutefet
                                         mem-chet-lamed
                                         meshutefet-place-proportion]}
                                 (-> feature
                                     :properties
                                     ((juxt :SEMEL_YISH :STAT11))
                                     place-stat2011->aggregation)]
                        (let [ratio (/ meshutefet (+ meshutefet mem-chet-lamed))]
                          (-> feature
                              (assoc :type "Feature")
                              (assoc-in [:properties :center]
                                        (-> feature
                                            :geometry
                                            jts/centroid
                                            point->yx))
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
                                        (format "<h1><p>%d</p><p>%d</p><p>%.02f%%</p></h1>"
                                                (math/round meshutefet)
                                                (math/round mem-chet-lamed)
                                                (* 100 meshutefet-place-proportion)))))))))
             (filter some?)
             vec)
        center (-> enriched-features
                   (->> (mapv (comp :center :properties)))
                   (tensor/reduce-axis fun/mean 0)
                   vec)]
    [:div {:style {:width "50%"}}
     [:h3 election]
     (choropleth-map
      {:provider "Stadia.AlidadeSmoothDark"
       #_"OpenStreetMap.Mapnik"
       :center center
       :zoom 14
       :enriched-features enriched-features})]))
