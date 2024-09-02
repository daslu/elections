(ns index
  (:require [fastmath.stats]
            [tablecloth.api :as tc]
            [tech.v3.datatype.functional :as fun]
            [tech.v3.tensor :as tensor]
            [tech.v3.dataset.print :as print]
            [scicloj.kindly.v4.kind :as kind]
            [clojure.string :as str]
            [charred.api :as charred]
            [com.rpl.specter :as specter]
            [geo
             [geohash :as geohash]
             [jts :as jts]
             [spatial :as spatial]
             [io :as geoio]
             [crs :as crs]]
            [clojure.walk :as walk]
            [clojure.math :as math]
            [tablecloth.column.api :as tcc])
  (:import (org.locationtech.jts.index.strtree STRtree)
           (org.locationtech.jts.geom Geometry Point Polygon Coordinate)
           (org.locationtech.jts.geom.prep PreparedGeometry
                                           PreparedLineString
                                           PreparedPolygon
                                           PreparedGeometryFactory)
           (java.util TreeMap)))


(defn normalize [xs]
  (fun// xs (fun/sum xs)))


;; https://www.jewfaq.org/hebrew_alphabet
(def Heb-letter->Eng-name
  {\א "aleph"
   \ב "bet"
   \ג "gimmel"
   \ד "dalet"
   \ה "hey"
   \ו "vav"
   \ז "zayin"
   \ח "chet"
   \ט "tet"
   \י "yod"
   \כ "kaf"
   \ל "lamed"
   \מ "mem"
   \ם "memsofit"
   \נ "nun"
   \ן "nunsofit"
   \ס "samech"
   \ע "ayin"
   \פ "fay"
   \ף "faysofit"
   \צ "tzadik"
   \ץ "tzadiksofit"
   \ק "kuf"
   \ר "resh"
   \ש "shin"
   \ת "taf"})

(def elections (range 21 26))

(def Heb-column-names->std
  {"שם ישוב" :place-name
   "סמל ישוב" :place-id
   "קלפי" :kalpi
   "מספר קלפי" :kalpi
   "בזב" :bzb
   "מצביעים" :voters
   "פסולים" :invalid
   "כשרים" :valid
   "סמל ועדה" :vaada-id
   "ברזל" :barzel
   "ריכוז" :ricuz
   "שופט" :judge})

(def votes
  (->> elections
       (map (fn [i]
              [i (-> (format "data/expb%s.csv" i)
                     (tc/dataset {:key-fn (fn [s]
                                            (or (Heb-column-names->std s)
                                                (->> s
                                                     (map Heb-letter->Eng-name)
                                                     (str/join "-")
                                                     keyword)))}))]))
       (into {})))

(def mapping-column-names->std
  {"kalpi_april_2019" :kalpi
   "kalpi_september_2019" :kalpi
   "kalpi2020" :kalpi
   "kalpi2021" :kalpi
   "kalpi2022" :kalpi
   "semel_yishuv" :place-id
   "shem_yishuv" :place-name
   "stat2011" :stat2011
   "yishuv_stat2011" :place-region})

(def mapping
  (->> elections
       (map (fn [i]
              [i (-> (format "data/mapping%s.csv" i)
                     (tc/dataset {:key-fn (comp mapping-column-names->std
                                                str/lower-case)})
                     (tc/drop-rows 0)
                     (tc/map-columns :stat2011 [:stat2011] #(Integer/parseInt %))
                     (tc/map-columns :place-id [:place-id] #(Integer/parseInt %))
                     (tc/map-columns :kalpi [:kalpi] #(Integer/parseInt %)))]))
       (into {})))


(def meshutefet-columns
  {21 [:vav-memsofit :dalet-ayin-memsofit]
   22 [:vav-dalet-ayin-memsofit]
   23 [:vav-dalet-ayin-memsofit]
   24 [:vav-dalet-ayin-memsofit :ayin-memsofit]
   25 [:vav-memsofit :dalet :ayin-memsofit]})

(comment
  (let [ks (-> (mapping 25)
               (tc/select-rows #(and (-> % :place-id (= 8600))
                                     (-> % :stat2011 (= 412))))
               :kalpi
               set)]
    (-> (votes 25)
        (tc/select-rows #(and (-> % :place-id (= 8600))
                              (-> % :kalpi math/round ks)))
        (tc/write! "/tmp/ramat-shikma.csv")))

  (let [ks (-> (mapping 25)
               (tc/select-rows #(and (-> % :place-id (= 8600))
                                     (-> % :stat2011 (= 418))))
               :kalpi
               set)]
    (-> (votes 25)
        (tc/select-rows #(and (-> % :place-id (= 8600))
                              (-> % :kalpi math/round ks)))
        (tc/write! "/tmp/ramat-hen.csv"))))


(def stat2011-aggregation
  (->> (range 21 26)
       (map (fn [election]
              [election
               (-> election
                   (votes)
                   (tc/add-column :meshutefet (fn [ds]
                                                (->> ds
                                                     ((apply juxt (meshutefet-columns
                                                                   election)))
                                                     (apply map fun/+))))
                   (tc/add-column :mahal-shas (fn [ds]
                                                (->> ds
                                                     ((apply juxt [:mem-chet-lamed :shin-samech]))
                                                     (apply map fun/+))))
                   (tc/add-column :kalpi (fn [ds]
                                           (fun/round (:kalpi ds))))
                   (tc/left-join (mapping election)
                                 [:place-name :place-id :kalpi])
                   #_(tc/select-rows #(-> % :place-name (= "עכו")))
                   (tc/select-columns [:place-name :place-id :kalpi :stat2011 :voters :meshutefet :mem-chet-lamed :mahal-shas])
                   (tc/group-by [:place-name :place-id :stat2011])
                   (tc/aggregate-columns [:voters :meshutefet :mem-chet-lamed :mahal-shas] fun/sum)
                   (tc/add-column :mahal-shas-vs-voters
                                  #(tcc// (:mahal-shas %)
                                          (:voters %)))
                   (tc/group-by [:place-name :place-id])
                   (tc/add-column :meshutefet-place-proportion
                                  (fn [place-ds]
                                    (-> place-ds
                                        :meshutefet
                                        normalize)))
                   (tc/add-column :mem-chet-lamed--place-proportion
                                  (fn [place-ds]
                                    (-> place-ds
                                        :mem-chet-lamed
                                        normalize)))
                   tc/ungroup
                   (print/print-range :all))]))
       (into {})))

(def election->place-stat2011->aggregation
  (-> stat2011-aggregation
      (update-vals
       (fn [ds]
         (-> ds
             (tc/rows :as-maps)
             (->> (map (fn [row]
                         [[(:place-id row)(:stat2011 row)]
                          (dissoc row :place-name :place-id :stat2011)]))
                  (into {})))))))




(defn slurp-gzip
  "Read a gzipped file into a string"
  [path]
  (with-open [in (java.util.zip.GZIPInputStream. (clojure.java.io/input-stream path))]
    (slurp in)))

(def stat2011-geojson
  (slurp-gzip "data/stat2011/Lamas_Census_Tracts_2011.geojson.gz"))

(def stat2011-features
  (geoio/read-geojson stat2011-geojson))

;; https://epsg.io/?q=Israel
(def crs-transform ; Israel1993->WGS84
  (geo.crs/create-transform (geo.crs/create-crs 2039)
                            (geo.crs/create-crs 4326)))

(defn Israel->WGS84 [geometry]
  (geo.jts/transform-geom geometry crs-transform))

(def stat2011-features-for-drawing
  (->> stat2011-features
       (mapv (fn [feature]
               (-> feature
                   (update :geometry Israel->WGS84))))))

(def Acre-center
  [32.92814000 35.07647000])

(def Lod-center
  [31.9467, 34.8903])

(defn choropleth-map [details]
  (kind/reagent
   ['(fn [{:keys [provider
                  center
                  enriched-features
                  zoom]}]
       [:div
        {:style {:height "1200px"}
         :ref   (fn [el]
                  (let [m (-> js/L
                              (.map el)
                              (.setView (clj->js center)
                                        zoom))]
                    (-> js/L
                        .-tileLayer
                        (.provider provider)
                        (.addTo m))
                    (-> js/L
                        (.geoJson (clj->js enriched-features)
                                  (clj->js {:style (fn [feature]
                                                     (-> feature
                                                         .-properties
                                                         .-style))}))
                        (.bindTooltip (fn [layer]
                                        (-> layer
                                            .-feature
                                            .-properties
                                            .-tooltip)
                                        ;; (fn [_]
                                        ;;   (clj->js
                                        ;;    {:permanent true}))
                                        ))
                        (.addTo m))))}])
    details]
   {:reagent/deps [:leaflet]}))



(->> stat2011-features-for-drawing
     (filter #(-> % :properties :SHEM_YISHU (= "עכו")))
     (mapv (fn [feature]
             (-> feature
                 (assoc :type "Feature")
                 (update :geometry
                         (fn [geometry]
                           (-> geometry
                               geoio/to-geojson
                               (charred/read-json {:key-fn keyword}))))))))



(defn point->yx [^Point point]
  (let [c (.getCoordinate point)]
    [(.getY c)
     (.getX c)]))


(->> stat2011-features-for-drawing
     (map #(-> % :properties :SHEM_YISHU))
     frequencies)

(delay
  (->>
   [21  25]
   (map
    (fn [election]
      (let [enriched-features
            (->> stat2011-features-for-drawing
                 (filter #(-> % :properties :SHEM_YISHU (= "נצרת עילית")))
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
                                         place-stat2011->aggregation)
                                     ;; ratio (/ meshutefet (+ meshutefet mem-chet-lamed))
                                     ]
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
                                           :fillOpacity (* 2 meshutefet-place-proportion)})
                                (assoc-in [:properties :tooltip]
                                          (format "<h1><p>%d</p><p>%.02f%%</p></h1>"
                                                  (math/round meshutefet)
                                                  (* 100 meshutefet-place-proportion))))))))
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
           :enriched-features enriched-features})])))
   (into [:div {:style {:display :flex
                        :width "2000px"}}])
   kind/hiccup))













(delay
  (->> ["עכו"
        "לוד"
        "רמלה"
        "נצרת עילית"
        "חיפה"]
       (mapcat (fn [y]
                 [(kind/hiccup [:h1 y])
                  (->>
                   [21  25]
                   (map
                    (fn [election]
                      (let [enriched-features
                            (->> stat2011-features-for-drawing
                                 (filter #(-> % :properties :SHEM_YISHU (= y)))
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
                           :enriched-features enriched-features})])))
                   (into [:div {:style {:display :flex
                                        :width "2000px"}}])
                   kind/hiccup)]))
       kind/fragment))
