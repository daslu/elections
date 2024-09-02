(ns h2008
  (:require [tablecloth.api :as tc]
            [tablecloth.column.api :as tcc]))


(defonce h2008
  (-> "/workspace/datasets/puf2008/PUF 2008/H20081171Data.csv"
      (tc/dataset {:key-fn keyword})
      time))

(-> h2008
    keys)

(delay
  (-> h2008
      (tc/select-rows #(-> % :SmlYishuvPUF (= 8600)))
      (tc/group-by [:SmlEzorStatistiKtvtMegurimPUF
                    :YabeshetMotzaByEmMchvPUF
                    :YabeshetMotzaByEmMchlkMchvPUF
                    :YabeshetMotzaByAvMchvPUF
                    :YabeshetMotzaByAvMchlkMchvPUF])
      (tc/aggregate {:n tc/row-count})
      (tc/select-rows #(-> %
                           :SmlEzorStatistiKtvtMegurimPUF
                           #{411 412}))
      (tc/write! "411-412.csv")))


(delay
  (-> h2008
      (tc/select-rows #(and (-> % :SmlYishuvPUF (= 8600))
                            (-> %
                                :SmlEzorStatistiKtvtMegurimPUF
                                #{411 412})))
      (tc/select-columns [:SmlEzorStatistiKtvtMegurimPUF
                          :YabeshetMotzaByEmMchvPUF
                          :YabeshetMotzaByEmMchlkMchvPUF
                          :YabeshetMotzaByAvMchvPUF
                          :YabeshetMotzaByAvMchlkMchvPUF])
      (tc/write! "411-412-respondents.csv")))



(delay
  (-> h2008
      (tc/select-rows #(-> % :SmlYishuvPUF (= 8600)))
      (tc/select-rows #(-> %
                           :SmlEzorStatistiKtvtMegurimPUF
                           #{411 412}))
      (tc/group-by [:SmlEzorStatistiKtvtMegurimPUF
                    :YabeshetMotzaByEmMchvPUF
                    :YabeshetMotzaByEmMchlkMchvPUF
                    :YabeshetMotzaByAvMchvPUF
                    :YabeshetMotzaByAvMchlkMchvPUF])
      (tc/aggregate {:n tc/row-count})
      (tc/group-by [:SmlEzorStatistiKtvtMegurimPUF])
      ;; (tc/aggregate (fn [:YabeshetMotzaByAvMchlkMchvPUF]))
      ))





(delay
  (-> h2008
      (tc/select-rows #(-> % :SmlYishuvPUF (= 8600)))
      (tc/select-rows #(-> %
                           :SmlEzorStatistiKtvtMegurimPUF
                           #{411 412}))
      (tc/group-by [:SmlEzorStatistiKtvtMegurimPUF
                    :GilPUF])
      (tc/aggregate {:n tc/row-count})
      (tc/order-by [:SmlEzorStatistiKtvtMegurimPUF
                    :GilPUF])
      (tc/group-by [:SmlEzorStatistiKtvtMegurimPUF])
      (tc/map-columns :age [:GilPUF] (comp ["0-14" "15-24" "25-34" "35-49" "50-69" "70+"]
                                           dec))
      (tc/add-column :percent (fn [ds]
                                (->  (:n ds)
                                     (tcc// (tcc/sum (:n ds)))
                                     (tcc/* 100))))
      tc/ungroup
      (tc/write! "/tmp/age.csv")))
