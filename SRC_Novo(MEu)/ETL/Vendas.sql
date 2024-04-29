-- Databricks notebook source
WITH tb_pedido_item AS (
SELECT t2.*,
       t1.dtPedido
FROM silver.olist.pedido as t1

LEFT JOIN silver.olist.item_pedido as t2
ON t1.idPedido = t2.idPedido 

WHERE t1.dtPedido < '2018-01-01'
AND t1.dtPedido >= add_months('2018-01-01', -6)
AND t2.idVendedor IS NOT NULL
),
tb_summary AS (
  SELECT  
          idVendedor,
          count(DISTINCT idPedido) AS qtdPedidos,
          count(DISTINCT date(dtPedido)) AS qtdDias,
          count(idProduto) AS qtdItens,
          min(datediff('2018-01-01', dtPedido)) AS qtrecencia,
          sum(vlPreco) / count(DISTINCT idPedido) as AvgTicket,
          avg(vlPreco) as avgValorProduto,
          max(vlPreco) as maxValorProduto,
          min(vlPreco) as minValorProduto,
          count(idProduto) / count(DISTINCT idPedido) AS avgProdutoPedido
          

  FROM tb_pedido_item
  GROUP BY idVendedor
),
tb_pedido_summary AS (
  SELECT idVendedor,
        idPedido,
        sum(vlPreco) AS vlPreco
    FROM tb_pedido_item

    GROUP BY  idVendedor,idPedido
),
tb_min_max AS (
SELECT idVendedor,
       min(vlPreco) AS minVlPedido,
       max(vlPreco) AS maxVlPedido
FROM tb_pedido_summary
GROUP BY idVendedor
),
tb_life AS (
  SELECT t2.idVendedor,
        sum(vlPreco) as LTV,
        max(date_diff('2018-01-01', t1.dtPedido)) AS qtdDiasBase
  FROM silver.olist.pedido as t1

  LEFT JOIN silver.olist.item_pedido as t2
  ON t1.idPedido = t2.idPedido 

  WHERE t1.dtPedido < '2018-01-01'
  AND t2.idVendedor IS NOT NULL

  GROUP BY t2.idVendedor
),
tb_pedido AS (
  SELECT DISTINCT idVendedor,
          date(dtPedido) AS dtPedido
  FROM tb_pedido_item
  ORDER BY 1,2
),
tb_lag AS(
  SELECT  *,
          LAG(dtPedido) OVER (PARTITION BY idVendedor ORDER BY dtPedido) AS lag1
  FROM tb_pedido
),
tb_intervalo AS (
  SELECT idVendedor,
        avg(date_diff(dtPedido , lag1)) avgIntervaloVendas

  FROM tb_lag
  GROUP BY idVendedor
)
SELECT '2018-01-01' AS dtRference,
       t1.*,
       t2.minVlPedido,
       t2.maxVlPedido,
       t3.LTV,
       t3.qtdDiasBase,
       t4.avgIntervaloVendas


FROM tb_summary AS t1

LEFT JOIN tb_min_max AS t2
ON t1.idVendedor = t2.idVendedor

LEFT JOIN tb_life AS t3
ON t1.idVendedor = t3.idVendedor

LEFT JOIN tb_intervalo AS t4
ON t1.idVendedor = t4.idVendedor
