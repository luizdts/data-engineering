import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Script generated for node produtos
produtos_node1707264584806 = glueContext.create_dynamic_frame.from_catalog(
    database="vendas",
    table_name="produtos_csv",
    transformation_ctx="produtos_node1707264584806",
)

# Script generated for node itensvenda
itensvenda_node1707264319859 = glueContext.create_dynamic_frame.from_catalog(
    database="vendas",
    table_name="itensvenda_csv",
    transformation_ctx="itensvenda_node1707264319859",
)

# Script generated for node vendedores
vendedores_node1707264633076 = glueContext.create_dynamic_frame.from_catalog(
    database="vendas",
    table_name="vendedores_csv",
    transformation_ctx="vendedores_node1707264633076",
)

# Script generated for node Clientes
Clientes_node1707264448843 = glueContext.create_dynamic_frame.from_catalog(
    database="vendas",
    table_name="clientes_csv",
    transformation_ctx="Clientes_node1707264448843",
)

# Script generated for node Vendas
Vendas_node1707264219807 = glueContext.create_dynamic_frame.from_catalog(
    database="vendas",
    table_name="vendas_csv",
    transformation_ctx="Vendas_node1707264219807",
)

# Script generated for node itensvendaMapping
itensvendaMapping_node1707264329120 = ApplyMapping.apply(
    frame=itensvenda_node1707264319859,
    mappings=[
        ("idproduto", "long", "idproduto_itensvenda", "long"),
        ("idvenda", "long", "idvenda_itensvenda", "long"),
        ("quantidade", "long", "quantidade", "long"),
        ("valorunitario", "double", "valorunitario", "double"),
        ("valortotal", "double", "valortotal", "double"),
        ("desconto", "double", "desconto", "double"),
    ],
    transformation_ctx="itensvendaMapping_node1707264329120",
)

# Script generated for node vendasMapping
vendasMapping_node1707264278220 = ApplyMapping.apply(
    frame=Vendas_node1707264219807,
    mappings=[
        ("idvenda", "long", "idvenda", "long"),
        ("idvendedor", "long", "idvendedor_vendas", "long"),
        ("idcliente", "long", "idcliente_vendas", "long"),
        ("data", "string", "data", "string"),
        ("total", "double", "total", "double"),
    ],
    transformation_ctx="vendasMapping_node1707264278220",
)

# Script generated for node JoinVendas_itensVenda
JoinVendas_itensVenda_node1707264387421 = Join.apply(
    frame1=vendasMapping_node1707264278220,
    frame2=itensvendaMapping_node1707264329120,
    keys1=["idvenda"],
    keys2=["idvenda_itensvenda"],
    transformation_ctx="JoinVendas_itensVenda_node1707264387421",
)

# Script generated for node JoinClientes
JoinClientes_node1707264464655 = Join.apply(
    frame1=Clientes_node1707264448843,
    frame2=JoinVendas_itensVenda_node1707264387421,
    keys1=["idcliente"],
    keys2=["idcliente_vendas"],
    transformation_ctx="JoinClientes_node1707264464655",
)

# Script generated for node JoinProduto
JoinProduto_node1707264604543 = Join.apply(
    frame1=produtos_node1707264584806,
    frame2=JoinClientes_node1707264464655,
    keys1=["idproduto"],
    keys2=["idproduto_itensvenda"],
    transformation_ctx="JoinProduto_node1707264604543",
)

# Script generated for node JoinVendedores
JoinVendedores_node1707264651040 = Join.apply(
    frame1=vendedores_node1707264633076,
    frame2=JoinProduto_node1707264604543,
    keys1=["idvendedor"],
    keys2=["idvendedor_vendas"],
    transformation_ctx="JoinVendedores_node1707264651040",
)

# Script generated for node Colunasfinais
Colunasfinais_node1707264761435 = ApplyMapping.apply(
    frame=JoinVendedores_node1707264651040,
    mappings=[
        ("nome", "string", "nome", "string"),
        ("produto", "string", "produto", "string"),
        ("preco", "double", "preco", "double"),
        ("cliente", "string", "cliente", "string"),
        ("estado", "string", "estado", "string"),
        ("sexo", "string", "sexo", "string"),
        ("status", "string", "status", "string"),
        ("data", "string", "data", "string"),
        ("total", "double", "total", "double"),
        ("quantidade", "long", "quantidade", "long"),
        ("valorunitario", "double", "valorunitario", "double"),
        ("valortotal", "double", "valortotal", "double"),
        ("desconto", "double", "desconto", "double"),
    ],
    transformation_ctx="Colunasfinais_node1707264761435",
)

# Script generated for node Datalake
Datalake_node1707264859189 = glueContext.write_dynamic_frame.from_options(
    frame=Colunasfinais_node1707264761435,
    connection_type="s3",
    format="glueparquet",
    connection_options={
        "path": "s3://datalakeengdados777/datalake/",
        "partitionKeys": ["status"],
    },
    format_options={"compression": "snappy"},
    transformation_ctx="Datalake_node1707264859189",
)

job.commit()
