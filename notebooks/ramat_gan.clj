^{:kindly/hide-code true}
(ns ramat-gan
  (:require [tablecloth.api :as tc]
            [tablecloth.column.api :as tc]
            [geo.jts :as jts]
            [charred.api :as charred]
            [geo.io :as geoio]
            [tech.v3.tensor :as tensor]
            [tech.v3.datatype.functional :as fun]
            [clojure.java.io :as io]
            [scicloj.kindly.v4.kind :as kind]
            index
            h2008
            [clojure.math :as math]
            [tablecloth.column.api :as tcc]))




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
                                    "darkpurple"
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

(->> (range 1 11)
     (map (fn [i]
            (kind/hiccup [:div {:style {:width "100px"
                                        :background-color "darkpurple"
                                        :color "white"
                                        :opacity (* 0.07 i)
                                        :margin "0px"}}
                          [:h4{:style {:margin "0px"}}
                           i]])))
     (into [:div])
     kind/fragment)

(->> (range 1 11)
     (map (fn [i]
            [:div {:background-color "darkpurple"}
             [:big [:p i]]]))
     (into [:div])
     kind/hiccup)

;; ### אחוז הצבעה לליכוד ולשס 2022

(delay
  (let [enriched-features
        (->> index/stat2011-features-for-drawing
             (filter #(-> % :properties :SEMEL_YISH (#{8600})))
             (map (fn [feature]
                    (when-let [place-stat2011->aggregation (index/election->place-stat2011->aggregation
                                                            25)]
                      (let [{:as aggregation
                             :keys [voters
                                    mahal-shas
                                    mahal-shas-vs-voters]}
                            (-> feature
                                :properties
                                ((juxt :SEMEL_YISH :STAT11))
                                place-stat2011->aggregation)
                            color (if mahal-shas-vs-voters
                                    "darkpurple"
                                    "darkred")
                            opacity (or
                                     mahal-shas-vs-voters
                                     0)]
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
                                      {:color color
                                       :fillColor color
                                       :opacity 1
                                       :weight 3
                                       :fillOpacity opacity})
                            (assoc-in [:properties :tooltip]
                                      (if mahal-shas-vs-voters
                                        (format "%d/%d=%f%%"
                                                (math/round mahal-shas)
                                                (math/round voters)
                                                (* 100 mahal-shas-vs-voters)))))))))
             (filter some?)
             vec)
        center (-> enriched-features
                   (->> (mapv (comp :center :properties)))
                   (tensor/reduce-axis fun/mean 0)
                   vec)]
    (index/choropleth-map
     {:provider "Stadia.AlidadeSmooth"
      :center center
      :zoom 14
      :enriched-features enriched-features})))


(let [sa-2011->demographics (-> h2008/h2008
                                (tc/select-rows #(-> % :SmlYishuvPUF (= 8600)))
                                (tc/group-by [:SmlEzorStatistiKtvtMegurimPUF])
                                (tc/aggregate (fn [ds]
                                                (let [yabashot (-> ds
                                                                   (tc/select-columns [:YabeshetMotzaByEmMchlkMchvPUF
                                                                                       :YabeshetMotzaByAvMchlkMchvPUF])
                                                                   tc/rows)]
                                                  {:mizrachi (->> yabashot
                                                                  (filter (fn [ys]
                                                                            (->> ys
                                                                                 (every? #{1 2}))))
                                                                  count)
                                                   :ashkenazi (->> yabashot
                                                                   (filter (fn [ys]
                                                                             (->> ys
                                                                                  (every? #{3 5}))))
                                                                   count)})))
                                (tc/rename-columns {:summary-ashkenazi :n-ashkenazi
                                                    :summary-mizrachi :n-mizrachi
                                                    :SmlEzorStatistiKtvtMegurimPUF :sa-2011})
                                (tc/add-columns {:n-relevant #(tcc/+ (:n-ashkenazi %)
                                                                     (:n-mizrachi %))})
                                (tc/add-columns {:p-mizrachi #(-> (:n-mizrachi %)
                                                                  (tcc/* 1.0)
                                                                  (tcc// (:n-relevant %)))
                                                 :p-ashkenazi #(-> (:n-ashkenazi %)
                                                                   (tcc/* 1.0)
                                                                   (tcc// (:n-relevant %)))})
                                (tc/group-by :sa-2011 {:result-type :as-map})
                                (update-vals (fn [ds]
                                               (-> ds
                                                   (update-vals only-one)))))
      enriched-features (->> index/stat2011-features-for-drawing
                             (filter #(-> % :properties :SHEM_YISHU (= "רמת גן")))
                             (map (fn [feature]
                                    (let [{:keys [p-ashkenazi]} (-> feature
                                                                    :properties
                                                                    :STAT11
                                                                    sa-2011->demographics)]
                                      (let [ratio (if p-ashkenazi
                                                    (* 0.7 (/ p-ashkenazi
                                                              10.0))
                                                    0)
                                            color (if p-ashkenazi
                                                    "darkpurple"
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
                                                      (format "<h1><p>%f</p></h1>"
                                                              p-ashkenazi)))))))
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
      :enriched-features enriched-features})]))
