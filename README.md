# Teste data-eng

## quick start 
    run pip install -r requirements.txt
    incluir o arquivo zip datatest (1).zip com o dataset diponibilizado no documento do teste

    run data_export.py



## cobertura 
objetivo do script e seus dados exportados
10 produtos com a maior variação de preço positivo baseado no capo "priceVariation" (exported_data/top_10_price_variations_positive_AAAA_MM_DD.csv)
10 produtos com a maior variação de preço negativo baseado no campo "priceVariation" (exported_data/top_10_price_variations_negative_AAAA_MM_DD.csv)
10 produtos com a menor disponibilidade baseado no campo "available" (exported_data/10_unavailable_AAAA_MM_DD.csv)
Arquivo com metricas gerais por produto (exported_data/10_unavailable_AAAA_MM_DD)

## ideia de execucao do script
poder receber os dados via FTP ou bucket S3
Usar uma imagen de docker com o script(data_export.py)
uar o cloudWatch para executar o comando a cada mes 
possibilidade de envio desses dados para terceiros caso necessario