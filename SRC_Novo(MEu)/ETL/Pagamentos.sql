-- Databricks notebook source
select * from silver.olist.pagamento_pedido

-- COMMAND ----------

with tb_join as (

 select t2. *,
       t3.idVendedor
 from silver.olist.pedido as t1
 left join silver.olist.pagamento_pedido as t2
 on t1.idPedido = t2.idPedido

 left join silver.olist.item_pedido as t3
 on t1.idPedido = t3.idPedido

 where t1.dtPedido <= '2018-01-01'
 and t1.dtPedido  >= add_months('2018-01-01', -6)
 AND T3.idVendedor IS NOT NULL
),

tb_group as (
 SELECT idVendedor,
        descTipoPagamento,
        count(distinct idPedido) as qtdePedidoMeioPagamento,
        sum(vlPagamento) as vlPedidoMeioPagamento

  FROM tb_join
  group by idVendedor, descTipoPagamento
  ORDER BY idVendedor, descTipoPagamento
)

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
