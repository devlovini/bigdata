from pyspark.sql import SparkSession
from pyspark.sql import Row
from pyspark.sql.functions import col, when


spark = SparkSession.builder \
    .appName("Controle de Estoque e Vendas") \
    .getOrCreate()


produtos_data = [
    Row(id=1, nome="Perfume A", quantidade=100, preco=99.90),
    Row(id=2, nome="Perfume B", quantidade=50, preco=89.90),
    Row(id=3, nome="Perfume C", quantidade=75, preco=79.90)
]

vendas_data = []


produtos_df = spark.createDataFrame(produtos_data)
vendas_df = spark.createDataFrame(vendas_data)

def consultar_estoque():
    print("Consulta de Estoque:")
    produtos_df.select("id", "nome", "quantidade").show()

def registrar_venda(id_produto, quantidade):
    global produtos_df, vendas_data
    produto = produtos_df.filter(col("id") == id_produto).first()
    
    if produto is None:
        print("Produto não encontrado.")
        return

    if produto.quantidade < quantidade:
        print("Estoque insuficiente.")
        return

    nova_quantidade = produto.quantidade - quantidade
    produtos_df = produtos_df.withColumn("quantidade", 
                                          when(col("id") == id_produto, nova_quantidade).otherwise(col("quantidade")))
    valor_total = produto.preco * quantidade
    print(f"Venda registrada: Produto {produto.nome}, Quantidade: {quantidade}, Valor Total: {valor_total}")

    nova_venda = Row(id_venda=len(vendas_data) + 1, id_produto=id_produto, quantidade=quantidade, valor_total=valor_total)
    vendas_df = vendas_df.union(spark.createDataFrame([nova_venda]))
    vendas_data.append(nova_venda)

def consultar_vendas():
    print("Consulta de Vendas:")
    vendas_df.show()


def menu():
    while True:
        print("\nMenu:")
        print("1. Consultar Estoque")
        print("2. Registrar Venda")
        print("3. Consultar Vendas")
        print("4. Sair")
        
        opcao = input("Escolha uma opção: ")
        
        if opcao == '1':
            consultar_estoque()
        elif opcao == '2':
            id_produto = int(input("Digite o ID do produto: "))
            quantidade = int(input("Digite a quantidade a ser vendida: "))
            registrar_venda(id_produto, quantidade)
        elif opcao == '3':
            consultar_vendas()
        elif opcao == '4':
            break
        else:
            print("Opção inválida. Tente novamente.")


menu()


spark.stop()
