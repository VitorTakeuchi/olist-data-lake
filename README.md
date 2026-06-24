## README — Projeto 1 (Power BI)

```markdown
# Dashboard Executivo de Vendas — E-Commerce Olist (Power BI)

Relatório interativo de 3 páginas analisando ~100k pedidos do e-commerce Olist:
receita, ticket médio, sazonalidade, performance por categoria e indicadores de entrega.

## Demo
- 🌐 Link público (Publish to web): [link, se disponível]
- 📄 PDF do relatório: ./relatorio.pdf
- 📊 Arquivo: ./olist_dashboard.pbix

## Modelo de dados
Modelo estrela: fato de itens de pedido + dimensões (data, produto, cliente, vendedor).
Tabela de pedidos enriquecida para KPIs de entrega e avaliação.
[print do diagrama do modelo]

## Medidas DAX (destaques)
- Total Revenue, Total Orders, Average Order Value (AOV)
- Revenue YoY %, Revenue MoM %, média móvel de 3 meses
- % de receita por categoria, ranking de categorias
- Avaliação média, prazo médio de entrega, taxa de entrega no prazo
(código completo em /dax/medidas.md)

## Fonte de dados
Camada gold do projeto companheiro "Data Lake de E-Commerce" (medalhão).
[link para o outro repo]

## Stack
Power BI Desktop, DAX, Power Query.
```

---
