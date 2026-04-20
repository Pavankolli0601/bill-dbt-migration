from diagrams import Diagram, Cluster, Edge
from diagrams.programming.language import Python
from diagrams.onprem.database import PostgreSQL
from diagrams.onprem.analytics import Dbt
from diagrams.onprem.container import Docker
from diagrams.saas.analytics import Snowflake

with Diagram(
    "BILL Financial Ops — dbt Migration Architecture",
    filename="docs/architecture",
    outformat="png",
    show=False,
    direction="LR"
):
    generator = Python("Data Generator\n500 invoices\n227 payments\n50 vendors")

    with Cluster("Snowflake — Source Engine"):
        with Cluster("Bronze"):
            bronze = Dbt("stg_invoices\nstg_payments\nstg_vendors\nstg_approvals")
        with Cluster("Silver"):
            silver = Dbt("int_invoice_payment\nint_vendor_spend\nint_approval_funnel")
        with Cluster("Gold"):
            gold = Dbt("mart_ap_aging\nmart_payment_velocity\nmart_vendor_spend")

    with Cluster("Trino — Target Engine"):
        with Cluster("Migrated Models"):
            trino = Docker("Same 10 models\nTrino SQL dialect\nMemory connector")

    generator >> bronze >> silver >> gold
    gold >> Edge(label="SQL dialect\nmigration") >> trino