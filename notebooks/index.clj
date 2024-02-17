(ns index
  (:require [fastmath.stats]
            [tablecloth.api :as tc]
            [tech.v3.datatype.functional :as fun]
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
            [clojure.walk :as walk])
  (:import (org.locationtech.jts.index.strtree STRtree)
           (org.locationtech.jts.geom Geometry Point Polygon Coordinate)
           (org.locationtech.jts.geom.prep PreparedGeometry
                                           PreparedLineString
                                           PreparedPolygon
                                           PreparedGeometryFactory)
           (java.util TreeMap)))



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


(def aggregated-by-stat2011
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
                   (tc/add-column :kalpi (fn [ds]
                                           (fun/round (:kalpi ds))))
                   (tc/left-join (mapping election)
                                 [:place-name :place-id :kalpi])
                   (tc/select-rows #(-> % :place-name (= "עכו")))
                   (tc/select-columns [:kalpi :stat2011 :meshutefet :mem-chet-lamed])
                   (tc/group-by [:stat2011])
                   (tc/aggregate-columns [:meshutefet :mem-chet-lamed] fun/sum)
                   (print/print-range :all))]))
       (into {})))

(def election->stat2011->counts
  (-> aggregated-by-stat2011
      (update-vals
       (fn [ds]
         (-> ds
             (tc/rows :as-maps)
             (->> (map (fn [row]
                         [(:stat2011 row)
                          (dissoc row :stat2011)]))
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

(defn Israel->WSG84 [geometry]
  (geo.jts/transform-geom geometry crs-transform))

(def stat2011-features-for-drawing
  (->> stat2011-features
       (mapv (fn [feature]
               (-> feature
                   (update :geometry Israel->WSG84))))))

(def Acre-center
  [32.92814000 35.07647000])

(defn choropleth-map [details]
  (kind/reagent
   ['(fn [{:keys [provider
                  center
                  enriched-features
                  zoom]}]
       [:div
        {:style {:height "900px"}
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
                        #_(.bindTooltip (fn [layer]
                                          (-> layer
                                              .-feature
                                              .-properties
                                              .-tooltip)))
                        (.addTo m))))}])
    details]
   {:reagent/deps [:leaflet]}))


(delay
  (choropleth-map
   {:provider "OpenStreetMap.Mapnik"
    :center Acre-center
    :zoom 13
    :enriched-features
    (->> stat2011-features-for-drawing
         (filter #(-> % :properties :SHEM_YISHU (= "עכו")))
         (mapv (fn [feature]
                 (-> feature
                     (assoc :type "Feature")
                     (update :geometry
                             (fn [geometry]
                               (-> geometry
                                   geoio/to-geojson
                                   (charred/read-json {:key-fn keyword}))))
                     (assoc-in [:properties :style]
                               {:color      "purple"
                                :fillColor  "purple"
                                :opacity 1
                                :fillOpacity 0.5})))))}))
