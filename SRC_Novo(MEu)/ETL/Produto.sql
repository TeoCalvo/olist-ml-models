-- Databricks notebook source
WITH tb_join  AS(

  SELECT DISTINCT
        t2.idVendedor,
        t3.*



    FROM silver.olist.pedido AS t1

    LEFT JOIN silver.olist.item_pedido as t2
    ON t1.idPedido = t2.idPedido

    LEFT JOIN silver.olist.produto as t3
    ON t2.idProduto = t3.idProduto

    WHERE t1.dtPedido < '2018-01-01'
    AND t1.dtPedido >= add_months('2018-01-01', -6)
    AND t2.idVendedor IS NOT NULL
),

tb_summary AS(
  SELECT idVendedor,
        avg(coalesce(nrFotos,0)) avgFotos,
        avg(vlComprimentoCm * vlAlturaCm * vlLarguraCm) as avgVolumeProduto,
        percentile(vlComprimentoCm * vlAlturaCm * vlLarguraCm, 0.5) as medianVolumeProduto,
        min(vlComprimentoCm * vlAlturaCm * vlLarguraCm) as maxVolumeProduto,
        max(vlComprimentoCm * vlAlturaCm * vlLarguraCm) as minVolumeProduto,


        COUNT(DISTINCT CASE WHEN descCategoria = 'cama_mesa_banho' THEN idProduto end) / COUNT(DISTINCT idProduto) as pctCategoriacama_mesa_banho,
        COUNT(DISTINCT CASE WHEN descCategoria = 'beleza_saude' THEN  idProduto end) / COUNT(DISTINCT idProduto) as pctCategoriabeleza_saude,
        COUNT(DISTINCT CASE WHEN descCategoria = 'esporte_lazer' THEN idProduto end) / COUNT(DISTINCT idProduto) as pctCategoriaesporte_laze,
        COUNT(DISTINCT CASE WHEN descCategoria = 'informatica_acessorios' THEN  idProduto end) / COUNT(DISTINCT idProduto) as pctCategoriainformatica_acessorios,
        COUNT(DISTINCT CASE WHEN descCategoria = 'moveis_decoracao' THEN  idProduto end) / COUNT(DISTINCT idProduto) as pctCategoriamoveis_decoracao,
        COUNT(DISTINCT CASE WHEN descCategoria = 'utilidades_domesticas' THEN idProduto end) / COUNT(DISTINCT idProduto) as pctCategoriautilidades_domestica,
        COUNT(DISTINCT CASE WHEN descCategoria = 'relogios_presentes' THEN  idProduto end) / COUNT(DISTINCT idProduto) as pctCategoriarelogios_presentes,
        COUNT(DISTINCT CASE WHEN descCategoria = 'telefonia' THEN idProduto end) / COUNT(DISTINCT idProduto) as pctCategoriatelefoni,
        COUNT(DISTINCT CASE WHEN descCategoria = 'automotivo' THEN  idProduto end) / COUNT(DISTINCT idProduto) as pctCategoriaautomotivo,
        COUNT(DISTINCT CASE WHEN descCategoria = 'brinquedos' THEN  idProduto end) / COUNT(DISTINCT idProduto) as pctCategoriabrinquedos,
        COUNT(DISTINCT CASE WHEN descCategoria = 'cool_stuff' THEN  idProduto end) / COUNT(DISTINCT idProduto) as pctCategoriacool_stuff,
        COUNT(DISTINCT CASE WHEN descCategoria = 'ferramentas_jardim' THEN  idProduto end) / COUNT(DISTINCT idProduto) as pctCategoriaferramentas_jardim,
        COUNT(DISTINCT CASE WHEN descCategoria = 'perfumaria' THEN  idProduto end) / COUNT(DISTINCT idProduto) as pctCategoriaperfumaria,
        COUNT(DISTINCT CASE WHEN descCategoria = 'bebes' THEN idProduto end) / COUNT(DISTINCT idProduto) as pctCategoriabebe,
        COUNT(DISTINCT CASE WHEN descCategoria = 'eletronicos' THEN idProduto end) / COUNT(DISTINCT idProduto) as pctCategoriaeletronico

    

    FROM tb_join

    GROUP BY idVendedor
  
        
)
SELECT '2018-01-01' AS dtReference,
        *

FROM tb_summary



-- COMMAND ----------


  SELECT descCategoria


    FROM silver.olist.item_pedido as t2
    
    LEFT JOIN silver.olist.produto as t3
    ON t2.idProduto = t3.idProduto

    WHERE  t2.idVendedor IS NOT NULL

    GROUP BY 1
    ORDER BY COUNT(DISTINCT t2.idPedido) DESC

    LIMIT 15
