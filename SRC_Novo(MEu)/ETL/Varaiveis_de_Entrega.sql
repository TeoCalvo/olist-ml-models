-- Databricks notebook source
WITH tb_pedido AS(

  SELECT t1.idPedido,
        t2.idVendedor,
        t1.descSituacao,
        t1.dtPedido,
        t1.dtAprovado,
        t1.dtEntregue,
        t1.dtEstimativaEntrega,
        sum(t2.vlFrete) as TotalFrete

  FROM silver.olist.pedido as t1

  LEFT JOIN silver.olist.item_pedido as t2
  ON t1.idPedido = t2.idPedido

  WHERE dtPedido < '2018-01-01'
  AND dtPedido >= add_months('2018-01-01', -6)
  AND idVendedor IS NOT NULL

  GROUP BY t1.idPedido,
        t2.idVendedor,
        t1.descSituacao,
        t1.dtPedido,
        t1.dtAprovado,
        t1.dtEntregue,
        t1.dtEstimativaEntrega
)

SELECT  '2018-01-01' AS dtReference,
        idVendedor,
        COUNT(DISTINCT CASE WHEN DATE(coalesce(dtEntregue,'2018-01-01') ) > DATE(dtEstimativaEntrega) THEN idPedido END)  / 
        COUNT (DISTINCT CASE WHEN descSituacao = 'delivered' AND DATE(dtEntregue) > DATE(dtEstimativaEntrega) THEN idPedido END) AS pctPedidoAtraso,
        COUNT(DISTINCT CASE WHEN descSituacao = 'canceled' THEN idPedido END) /  COUNT(DISTINCT idPedido) AS pctPedidoCancelado,
        avg(TotalFrete) as avgFrete,
        percentile(TotalFrete,0.5) as medianFrete,
        max(TotalFrete) as maxFrete,
        min(TotalFrete) as minFrete,
        avg(date_diff(coalesce(dtEntregue, '2018-01-01'),dtAprovado)) as qtdDiasAprovadoEntrega,
        avg(date_diff(coalesce(dtEntregue, '2018-01-01'),dtPedido)) as qtdDiasPedidoEntrega,
        avg(date_diff(dtEstimativaEntrega, coalesce(dtEntregue, '2018-01-01'))) AS qtdDiasEntregaPromessa
        


FROM tb_pedido

GROUP BY 1
