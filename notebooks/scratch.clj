(ns scratch
  (:require [fastmath.stats]
            [tablecloth.api :as tc]
            [scicloj.noj.v1.datasets :as datasets]
            [scicloj.kindly.v4.kind :as kind]))


;; https://www.jewfaq.org/hebrew_alphabet
(def Heb-letter->Eng-name
  {"א" :aleph
   "ב" :bet
   "ג" :gimmel
   "ד" :dalet
   "ה" :hey
   "ו" :vav
   "ז" :zayin
   "ח" :chet
   "ט" :tet
   "י" :yod
   "כ" :kaf
   "ל" :lamed
   "מ" :mem
   "ם" :memsofit
   "נ" :nun
   "ן" :nunsofit
   "ס" :samech
   "ע" :ayin
   "פ" :fay
   "ף" :faysofit
   "צ" :tzadik
   "ץ" :tzadiksofit
   "ק" :kuf
   "ר" :resh
   "ש" :shin
   "ת" :taf})


(def elections (range 21 26))

(def votes
  (->> elections
       (map (fn [i]
              [i (->> i
                      (format "data/expb%s.csv")
                      tc/dataset)]))
       (into {})))

(def mapping
  (->> elections
       (map (fn [i]
              [i (->> i
                      (format "data/mapping%s.csv")
                      tc/dataset)]))
       (into {})))
