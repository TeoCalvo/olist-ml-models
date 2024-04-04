-- Databricks notebook source
SELECT 
DISTINCT
t1.idPedido,
t2.idVendedor
FROM silver.olist.pedido AS t1

LEFT JOIN silver.olist.item_pedido AS t2
ON T1.idPedido = T2.idPedido
WHERE t1.dtPedido < '2018-01-01'
AND t1.dtPedido >= add_months('2018-01-01', -6)

-- COMMAND ----------

 WITH tb_pedidos AS (
SELECT 
  DISTINCT
  t1.idPedido,
  t2.idVendedor
FROM silver.olist.pedido AS t1

LEFT JOIN silver.olist.item_pedido AS t2
ON T1.idPedido = T2.idPedido

WHERE t1.dtPedido < '2018-01-01'
AND t1.dtPedido >= add_months('2018-01-01', -6)
AND t2.idVendedor IS NOT NULL
),



tb_join as (

 select t1.idVendedor,
        t2. *
 from tb_pedidos as t1
 left join silver.olist.pagamento_pedido as t2
 on t1.idPedido = t2.idPedido
),

tb_group as (
 SELECT idVendedor,
        descTipoPagamento,
        count(distinct idPedido) as qtdePedidoMeioPagamento,
        sum(vlPagamento) as vlPedidoMeioPagamento

  FROM tb_join
  group by idVendedor, descTipoPagamento
  ORDER BY idVendedor, descTipoPagamento
),

tb_summary AS(

 SELECT idVendedor,
  SUM(CASE WHEN descTipoPagamento = 'boleto' THEN qtdePedidoMeioPagamento else 0 END) AS qtde_boleto_pedido,
  SUM(CASE WHEN descTipoPagamento = 'credit_card' THEN qtdePedidoMeioPagamento else 0 END) AS qtde_credit_card_pedido,
  SUM(CASE WHEN descTipoPagamento = 'voucher' THEN qtdePedidoMeioPagamento else 0 END) AS qtde_voucher_pedido,
  SUM(CASE WHEN descTipoPagamento = 'debit_card' THEN qtdePedidoMeioPagamento else 0 END) AS debit_card_pedido,

  SUM(CASE WHEN descTipoPagamento = 'boleto' THEN vlPedidoMeioPagamento else 0 END) AS valor_boleto_pedido,
  SUM(CASE WHEN descTipoPagamento = 'credit_card' THEN vlPedidoMeioPagamento else 0 END) AS valor_credit_card_pedido,
  SUM(CASE WHEN descTipoPagamento = 'voucher' THEN vlPedidoMeioPagamento else 0 END) AS valor_voucher_pedido,
  SUM(CASE WHEN descTipoPagamento = 'debit_card' THEN vlPedidoMeioPagamento else 0 END) AS valor_card_pedido,

  SUM(CASE WHEN descTipoPagamento = 'boleto' THEN qtdePedidoMeioPagamento else 0 END) / SUM(qtdePedidoMeioPagamento)  AS pct_qtd_boleto_pedido,
  SUM(CASE WHEN descTipoPagamento = 'credit_card' THEN qtdePedidoMeioPagamento else 0 END) / SUM(qtdePedidoMeioPagamento)  AS pct_qtd_credit_card_pedido,
  SUM(CASE WHEN descTipoPagamento = 'voucher' THEN qtdePedidoMeioPagamento else 0 END) / SUM(qtdePedidoMeioPagamento)  AS pct_qtd_voucher_pedido,
  SUM(CASE WHEN descTipoPagamento = 'debit_card' THEN qtdePedidoMeioPagamento else 0 END) / SUM(qtdePedidoMeioPagamento)  AS pct_qtdt_card_pedido,

  SUM(CASE WHEN descTipoPagamento = 'boleto' THEN vlPedidoMeioPagamento else 0 END) / SUM(vlPedidoMeioPagamento) AS pct_valor_boleto_pedido,
  SUM(CASE WHEN descTipoPagamento = 'credit_card' THEN vlPedidoMeioPagamento else 0 END) / SUM(vlPedidoMeioPagamento) AS pct_valor_credit_card_pedido,
  SUM(CASE WHEN descTipoPagamento = 'voucher' THEN vlPedidoMeioPagamento else 0 END) / SUM(vlPedidoMeioPagamento) AS pct_valor_voucher_pedido,
  SUM(CASE WHEN descTipoPagamento = 'debit_card' THEN vlPedidoMeioPagamento else 0 END) / SUM(vlPedidoMeioPagamento) AS pct_valor_card_pedido

 FROM tb_group
 GROUP BY 1
),

tb_cartao AS (
  SELECT 
    idVendedor,
    AVG(nrParcelas) as avgQtdParcelas,
    PERCENTILE(nrParcelas, 0.5) AS  medianQtdParcelas,
    MAX(nrParcelas) as maxQtdParcelas,
    MIN(nrParcelas) as minQtdParcelas

  FROM tb_join
  WHERE descTipoPagamento = 'credit_card'
  GROUP BY idVendedor
)

SELECT 
       '2018-01-01' as dtReference,
       t1.*,
       t2.avgQtdParcelas,
       t2.medianQtdParcelas,
       t2.maxQtdParcelas,
       t2.minQtdParcelas

FROM tb_summary as t1

LEFT JOIN tb_cartao as t2
ON t1.idVendedor = t2.idVendedor 

