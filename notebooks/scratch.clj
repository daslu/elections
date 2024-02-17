(ns scratch
  (:require [fastmath.stats]
            [tablecloth.api :as tc]
            [tech.v3.datatype.functional :as fun]
            [tech.v3.dataset.print :as print]
            [scicloj.kindly.v4.kind :as kind]
            [clojure.string :as str]))


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
       (mapv
        (fn [election]
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
              (print/print-range :all))))))
