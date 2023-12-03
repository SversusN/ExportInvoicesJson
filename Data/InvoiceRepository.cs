using Dapper;
using System.Data.SqlClient;
using System.Data;
using System.Configuration;
using System.Collections.Generic;
using System.Threading.Tasks;
using System.Linq;

namespace ExportInvoicesJson
{
    public class InvoiceRepository 
    {
        private string _connectionString = ConfigurationManager.ConnectionStrings["connectionString"].ConnectionString;

        /// <summary>
        /// Возвращает список накладных, для которых необходимо сканирование КИЗ.
        /// Накладные без КИЗ или полностью обработанные не возвращаются.
        /// </summary>
        /// <param name="searchString">Строка поиска по внутреннему или внешнему номеру накладной.</param>
        /// <returns></returns>
        public async  Task<IEnumerable<Invoice>> FindAsync(string searchString)
        {
            string query = @"
                            with cte (id, doc_number, doc_date, supplier_number, supplier_date, supplier_sum, supplier_name)
                            as (
                            SELECT 
                                i.ID_INVOICE_GLOBAL,
                                i.MNEMOCODE,
                                i.DOCUMENT_DATE,
                                i.INCOMING_NUMBER,
                                i.INCOMING_DATE,
                                i.SUM_SUPPLIER,
                                contr.NAME
                            FROM dbo.INVOICE i
                            inner join dbo.CONTRACTOR contr on contr.ID_CONTRACTOR = i.ID_CONTRACTOR_SUPPLIER
                            WHERE i.DOCUMENT_STATE = 'SAVE'
                            )
                            select
                                [Id] = c.id,
                                [DocNumber] = c.doc_number + case when ttt.QuantityKizFact > 0 then 'y' else 'n' end,
                                [DocDate] = c.doc_date,
                                [SupplierNumber] = c.supplier_number,
                                [SupplierDate] = c.supplier_date,
  	                            [SupplierSum] = c.supplier_sum,
  	                            [SupplierName] = c.supplier_name
                            from cte c
                            join (
                                select -- оставляем только накладные с КИЗ, для которых нужно сканирование
  	                                [Id] = ii.ID_INVOICE_GLOBAL,
	                                [QuantityKizTotal] = SUM(ii.QUANTITY),
  	                                [QuantityKizFact] = SUM(isnull(kd.Quantity, 0))-- OVER (PARTITION BY ii.ID_INVOICE_GLOBAL)
                                from dbo.INVOICE_ITEM ii
                                left join (
  	                                select
  		                                [Id] = k2di.ID_DOCUMENT_ITEM_ADD,
	  	                                [Quantity] = sum(k2di.QUANTITY)
  	                                from dbo.KIZ_2_DOCUMENT_ITEM k2di
	                                where k2di.ID_DOCUMENT_ITEM_ADD in (select ii2.ID_INVOICE_item_GLOBAL from dbo.INVOICE_ITEM ii2 where ii2.ID_INVOICE_GLOBAL in (select Id from cte))
  	                                group by k2di.ID_DOCUMENT_ITEM_ADD
                                ) kd on ii.ID_INVOICE_ITEM_GLOBAL = kd.Id
                                where ii.ID_INVOICE_GLOBAL in (select Id from cte)
  	                                and isnull(ii.IS_KIZ, 0) = 1
                                group by ii.ID_INVOICE_GLOBAL
                            ) ttt on ttt.Id = c.Id and (ttt.QuantityKizTotal - QuantityKizFact) > 0
";

            // дописываем условие, если оно непустое, иначе возвращаем список целиком
            if (!string.IsNullOrWhiteSpace(searchString))
            {
                query += @"
                            where c.doc_number like @searchString
                                or c.supplier_number like @searchString
                            ";
            }

            // сортировка
            query += @"
                    order by c.doc_date desc
                    ";

            await using var connection = new SqlConnection(_connectionString);
            return await connection.QueryAsync<Invoice>(query, new { searchString = $"%{searchString}%" });
        }

        /// <summary>
        /// Возвращает полный список позиций накладной с признаком КИЗ и количеством КИЗ, которое уже есть в БД.
        /// Список КИЗ для конкретной позиции накладной загружается отдельно.
        /// </summary>
        /// <param name="invoiceId">guid накладной</param>
        /// <returns></returns>
        public async Task<IEnumerable<InvoiceItem>> GetItemsAsync(Guid invoiceId)
        {
            const string query = @"
                                    SELECT [ItemId] = k2di.ID_DOCUMENT_ITEM_ADD,
                                        [KizBase64] = k.BARCODE INTO #ItemsKiz
                                    FROM dbo.KIZ_2_DOCUMENT_ITEM k2di
                                        INNER JOIN dbo.KIZ k ON k.ID_KIZ_GLOBAL = k2di.ID_KIZ_GLOBAL
                                    WHERE k2di.ID_DOCUMENT_ITEM_ADD IN (
                                            SELECT ii2.ID_INVOICE_ITEM_GLOBAL
                                            FROM dbo.INVOICE_ITEM ii2
                                            WHERE ii2.ID_INVOICE_GLOBAL = @invoiceId
                                        );

                                    WITH kizQuantities AS (
                                        SELECT [Id] = ik.ItemId,
                                            [Quantity] = COUNT(ik.ItemId)
                                        FROM #ItemsKiz ik
                                        GROUP BY ik.ItemId
                                    ),
                                    baseGtinTable AS (
                                        SELECT distinct [Gtin] = MAX(kiz.GTIN),
                                            [GoodsId] = lot.ID_GOODS
                                        FROM goods
                                            INNER JOIN lot ON goods.ID_GOODS = lot.ID_GOODS
                                            INNER JOIN KIZ_2_DOCUMENT_ITEM ON lot.ID_DOCUMENT_ITEM_ADD = KIZ_2_DOCUMENT_ITEM.ID_DOCUMENT_ITEM_ADD
                                            INNER JOIN KIZ ON KIZ.ID_KIZ_GLOBAL = KIZ_2_DOCUMENT_ITEM.ID_KIZ_GLOBAL
                                        GROUP BY lot.ID_GOODS
                                    )
                                    SELECT [Id] = ii.ID_INVOICE_ITEM_GLOBAL,
                                        [ProductName] = g.NAME,
                                        [RetailPrice] = ii.RETAIL_PRICE_VAT,
                                        [SupplierPrice] = ii.SUPPLIER_PRICE_VAT,
                                        [SerialNumber] = s.SERIES_NUMBER,
                                        [ExpirationDate] = s.BEST_BEFORE,
                                        [Quantity] = CAST(ii.QUANTITY AS int),
                                        [KizQuantity] = CAST(ISNULL(kq.Quantity, 0) AS int),
                                        [Kiz] = ISNULL(ii.IS_KIZ, 0),
                                        [BaseGtin] = ISNULL(
                                            ii.GTIN,
                                            (
                                                SELECT top 1 bc.code
                                                FROM bar_code bc
                                                WHERE bc.ID_GOODS = g.id_goods
                                                    and bc.BAR_CODE_TYPE = 'KIZ'
                                            )
       
                                        )
                                    FROM dbo.INVOICE_ITEM ii
                                        JOIN dbo.GOODS g ON g.ID_GOODS = ii.ID_GOODS
                                        LEFT JOIN dbo.SERIES s ON s.ID_SERIES = ii.ID_SERIES
                                        LEFT JOIN kizQuantities kq ON kq.Id = ii.ID_INVOICE_ITEM_GLOBAL
                                        LEFT JOIN baseGtinTable ON baseGtinTable.GoodsId = g.ID_GOODS
                                    WHERE ii.ID_INVOICE_GLOBAL = @invoiceId

                                    SELECT [ItemId],
                                        [KizBase64]
                                    FROM #ItemsKiz
";

            await using var connection = new SqlConnection(_connectionString);
            using var reader = await connection.QueryMultipleAsync(query, new { invoiceId });
            var items = reader.Read<InvoiceItem>();
            var kizList = reader.Read<InvoiceItemKiz>();
            if (kizList != null && kizList.Any())
            {
                foreach (var itemKiz in kizList.GroupBy(x => x.ItemId))
                {
                    var invoiceItem = items.FirstOrDefault(x => x.Id == itemKiz.Key);
                    if (invoiceItem != null)
                    {
                        invoiceItem.KizList = string.Join(";", itemKiz.Select(x => x.KizBase64));
                    }
                }
            }
            return items;
        }


        public async Task AddKizAsync(Guid invoiceItemId, string gtin, string sn, string kizBase64)
        {
            if (string.IsNullOrWhiteSpace(gtin)) throw new ArgumentNullException(nameof(gtin));
            if (string.IsNullOrWhiteSpace(sn)) throw new ArgumentNullException(nameof(sn));
            if (string.IsNullOrWhiteSpace(kizBase64)) throw new ArgumentNullException(nameof(kizBase64));

            const string kizQuery = @"
IF not exists (select top 1 1 from kiz where barcode = @kizBase64)
BEGIN
INSERT INTO dbo.KIZ
(
    ID_KIZ_GLOBAL,
	BARCODE,
	GTIN_SGTIN,
	GTIN,
	SGTIN,
	INSERTED,
	UPDATED,
	ID_CONTRACTOR_GLOBAL,
	QUANTITY,
    IS_SSCC
)
VALUES
(
    @kizId,
    @kizBase64,
    @gtin + @sn,
    @gtin,
    @sn,
    GETDATE(),
    GETDATE(),
    dbo.FN_CONTRACTOR_SELF_GLOBAL(),
    1,
    0
)
END
IF  exists (select top 1 1 from kiz where barcode = @kizBase64)
set @kizId = (select top 1 id_kiz_global from kiz where barcode = @kizBase64)
BEGIN
INSERT INTO dbo.KIZ_2_DOCUMENT_ITEM
(
    ID_DOCUMENT_ITEM_ADD,
    ID_TABLE_ADD,
    ID_KIZ_GLOBAL,
    NUMERATOR,
    DENOMINATOR,
    QUANTITY
)
VALUES
(
    @invoiceItemId,
    2,
    @kizId,
    1,
    1,
    1
)
END
";

            var kizId = Guid.NewGuid();

            await using var connection = new SqlConnection(_connectionString);
            await connection.OpenAsync();
            await connection.ExecuteAsync(kizQuery, new
            {
                kizId,
                kizBase64,
                gtin,
                sn,
                invoiceItemId
            });
        }

        public async Task DeleteKizAsync(string gtinSn)
        {
            if (string.IsNullOrWhiteSpace(gtinSn)) throw new ArgumentNullException(nameof(gtinSn));

            const string query = @"
declare
	@IdKiz uniqueidentifier
	
select top 1
	@IdKiz = k.ID_KIZ_GLOBAL
from dbo.KIZ k
where k.GTIN_SGTIN = @gtinSn

delete from dbo.KIZ_2_DOCUMENT_ITEM
where ID_KIZ_GLOBAL = @IdKiz

delete from dbo.KIZ
where ID_KIZ_GLOBAL = @IdKiz
";

            await using var connection = new SqlConnection(_connectionString);
            await connection.OpenAsync();
            await connection.ExecuteAsync(query, new
            {
                gtinSn
            });
        }

        public async Task AddOrderKizAsync(string idDocument,string documentId, string date, string kizBase64)
        {
            if (string.IsNullOrWhiteSpace(documentId)) throw new ArgumentNullException(nameof(documentId));
            if (string.IsNullOrWhiteSpace(kizBase64)) throw new ArgumentNullException(nameof(kizBase64));

            const string kizQuery = @"
IF not exists (select top 1 id_internet_order_global from internet_order where barcode = @idDocument)
BEGIN

INSERT INTO [dbo].[INTERNET_ORDER]
           ([ID_INTERNET_ORDER_GLOBAL]
           ,[ID_CONTRACTOR]
           ,[NUMBER_DOC]
           ,[DATE_CREATE]
           ,[DOCUMENT_STATE]
           ,[ID_CONTRACTOR_OWNER]
           ,[DATE_MODIFIED]
           ,[CUSTOMER_NAME])
SELECT
@idDocument,
(select id_contractor from contractor where id_contractor_global = DBO.FN_CONTRACTOR_SELF_GLOBAL()),
'',
getdate(),
'SAVE',
(select id_contractor from contractor where id_contractor_global = DBO.FN_CONTRACTOR_SELF_GLOBAL()),
getdate(),
'ФАП'
END

INSERT INTO [INTERNET_ORDER_ITEM](
       [ID_INTERNET_ORDER_ITEM_GLOBAL]
      ,[ID_INTERNET_ORDER_GLOBAL]
      ,[ID_LOT_GLOBAL]
      ,[ID_GOODS]
      ,[QUANTITY]
      ,[SC_PAID]
      ,[INTERNAL_BARCODE]
      ,[INCOME_SOURCE]
      ,[ISG_REQUEST]
      ,[RETAIL_PRICE_IN_STOCKS])
  select
  @documentId,
  @idDocument,
  lot.ID_LOT_GLOBAL,
  lot.ID_GOODS,
  1,
  NULL,
  lot.INTERNAL_BARCODE,
  NULL,
  NULL,
  lot.PRICE_SAL

  FROM KIZ 
  INNER JOIN KIZ_2_DOCUMENT_ITEM k2 on kiz.ID_KIZ_GLOBAL = k2.ID_KIZ_GLOBAL
  INNER JOIN LOT on lot.ID_DOCUMENT_ITEM_ADD = k2.ID_DOCUMENT_ITEM_ADD
  where KIZ.BARCODE = @kizBase64



INSERT INTO [dbo].[KIZ_2_DOCUMENT_ITEM]
           ([ID_DOCUMENT_ITEM_ADD_PREV]
           ,[ID_DOCUMENT_ITEM_ADD]
           ,[ID_TABLE_ADD]
           ,[ID_KIZ_GLOBAL]
           ,[NUMERATOR]
           ,[DENOMINATOR]
           ,[QUANTITY]
           ,[REMAIN]
           ,[DATE_MODIFIED]
           ,[BARCODE])
     SELECT top 1
	 k2d.ID_DOCUMENT_ITEM_ADD,
	 @documentId,
	 null,
	 KIZ.ID_KIZ_GLOBAL,
	 1,
	 1,
	 1,
	 1,
	 getdate(),
	 null
FROM KIZ
left join (select top 1 ID_KIZ_GLOBAL, ID_DOCUMENT_ITEM_ADD, NUMERATOR, DENOMINATOR,DATE_MODIFIED
from KIZ_2_DOCUMENT_ITEM k2
order by DATE_MODIFIED desc) k2d on kiz.ID_KIZ_GLOBAL = k2d.ID_KIZ_GLOBAL
where kiz.BARCODE = @kizBase64
";

            var internetOrderId = Guid.NewGuid();

            await using var connection = new SqlConnection(_connectionString);
            await connection.OpenAsync();
            await connection.ExecuteAsync(kizQuery, new
            {
               idDocument,
               documentId,
               date,
               kizBase64
              
            });
        }

        public async Task AddInvKizAsync(string idDocument, string documentId, string kizBase64)
        {
            if (string.IsNullOrWhiteSpace(idDocument)) throw new ArgumentNullException(nameof(idDocument));
            if (string.IsNullOrWhiteSpace(documentId)) throw new ArgumentNullException(nameof(documentId));
            if (string.IsNullOrWhiteSpace(kizBase64)) throw new ArgumentNullException(nameof(kizBase64));

            const string kizQuery = @"
IF NOT EXISTS(
SELECT TOP 1 NULL FROM LOT
INNER JOIN INVENTORY_VED_ITEM IVI ON IVI.ID_LOT_GLOBAL = LOT.ID_LOT_GLOBAL
INNER JOIN INVENTORY_VED IV ON IV.ID_INVENTORY_VED_GLOBAL = IVI.ID_INVENTORY_VED_GLOBAL
INNER JOIN KIZ_2_DOCUMENT_ITEM K2DI ON K2DI.ID_DOCUMENT_ITEM_ADD = LOT.ID_DOCUMENT_ITEM_ADD
INNER JOIN KIZ ON KIZ.ID_KIZ_GLOBAL = K2DI.ID_KIZ_GLOBAL
WHERE KIZ.BARCODE = @kizBase64 AND IV.ID_INVENTORY_VED_GLOBAL  = @idDocument)
BEGIN
INSERT INTO 
[dbo].[INVENTORY_VED_ITEM]
           ([ID_INVENTORY_VED_ITEM_GLOBAL]
           ,[ID_INVENTORY_VED_GLOBAL]
           ,[ID_LOT_GLOBAL]
           ,[ID_GOODS]
           ,[ID_SCALING_RATIO]
           ,[QUANTITY]
           ,[VAT_PROD]
           ,[PVAT_PROD]
           ,[PRICE_PROD]
           ,[VAT_SUP]
           ,[PVAT_SUP]
           ,[PRICE_SUP]
           ,[SVAT_SUP]
           ,[SUM_SUP]
           ,[VAT_SAL]
           ,[PVAT_SAL]
           ,[PRICE_SAL]
           ,[SVAT_SAL]
           ,[SUM_SAL]
           ,[HAS_LOT]
           ,[CODE_STU]
           ,[ID_SUPPLIER]
           ,[INTERNAL_BARCODE]
           ,[ID_SERIES]
           ,[ADPRICE_SUP]
           ,[ADPRICE_SAL]
           ,[ID_REG_CERT_GLOBAL]
           ,[INCOMING_NUM]
           ,[INCOMING_BILL_NUM]
           ,[INCOMING_DATE]
           ,[INCOMING_BILL_DATE]
           ,[INVOICE_NUM]
           ,[INVOICE_DATE]
           ,[GTD_NUMBER]
           ,[BOX]
           ,[ID_STORE_PLACE]
           ,[REGISTER_PRICE]
           ,[ID_CONTRACT]
           ,[ID_GOSCONTRACT_FOR_SUPPLY]
           ,[ID_GOS_CONTRACT_SERVICE_GLOBAL])
select top 1
@documentId,
@idDocument,
lot.ID_LOT_GLOBAL,
lot.ID_GOODS,
lot.ID_SCALING_RATIO,
1,
vat_prod,
pvat_prod,
price_prod,
vat_sup,
pvat_sup,
price_sup,
0,
0,
VAT_SAL,
pvat_sal,
PRICE_SAL,
0,
0,
1,
'',
lot.ID_SUPPLIER,
lot.INTERNAL_BARCODE,
lot.ID_SERIES,
lot.ADPRICE_SUP,
lot.ADPRICE_SAL,
lot.ID_REG_CERT_GLOBAL,
lot.INCOMING_NUM,
lot.INCOMING_BILL_NUM,
lot.INCOMING_DATE,
lot.INCOMING_BILL_DATE,
lot.INVOICE_NUM,
lot.INVOICE_DATE,
lot.GTD_NUMBER,
lot.box,
lot.ID_STORE_PLACE,
lot.REGISTER_PRICE,
null,
null,
null
from lot
inner join KIZ_2_DOCUMENT_ITEM k2  on lot.ID_DOCUMENT_ITEM_ADD = k2.ID_DOCUMENT_ITEM_ADD
inner join kiz on kiz.ID_KIZ_GLOBAL = k2.ID_KIZ_GLOBAL
where kiz.barcode = @kizBase64 
order by k2.date_modified desc
END

ELSE 
BEGIN
UPDATE IVI
SET IVI.QUANTITY = IVI.QUANTITY+1

 FROM LOT
INNER JOIN INVENTORY_VED_ITEM IVI ON IVI.ID_LOT_GLOBAL = LOT.ID_LOT_GLOBAL
INNER JOIN INVENTORY_VED IV ON IV.ID_INVENTORY_VED_GLOBAL = IVI.ID_INVENTORY_VED_GLOBAL
INNER JOIN KIZ_2_DOCUMENT_ITEM K2DI ON K2DI.ID_DOCUMENT_ITEM_ADD = LOT.ID_DOCUMENT_ITEM_ADD
INNER JOIN KIZ ON KIZ.ID_KIZ_GLOBAL = K2DI.ID_KIZ_GLOBAL
WHERE KIZ.BARCODE = @kizBase64 AND IV.ID_INVENTORY_VED_GLOBAL  = @idDocument
END



INSERT INTO [dbo].[KIZ_2_DOCUMENT_ITEM]
           ([ID_DOCUMENT_ITEM_ADD_PREV]
           ,[ID_DOCUMENT_ITEM_ADD]
           ,[ID_TABLE_ADD]
           ,[ID_KIZ_GLOBAL]
           ,[NUMERATOR]
           ,[DENOMINATOR]
           ,[QUANTITY]
           ,[REMAIN]
           ,[DATE_MODIFIED]
           ,[BARCODE])
     SELECT top 1
	 k2d.ID_DOCUMENT_ITEM_ADD,
	 @documentId,
	 null,
	 KIZ.ID_KIZ_GLOBAL,
	 1,
	 1,
	 1,
	 1,
	 getdate(),
	 null
FROM KIZ
left join (select top 1 ID_KIZ_GLOBAL, ID_DOCUMENT_ITEM_ADD, NUMERATOR, DENOMINATOR,DATE_MODIFIED
from KIZ_2_DOCUMENT_ITEM k2
order by DATE_MODIFIED desc) k2d on kiz.ID_KIZ_GLOBAL = k2d.ID_KIZ_GLOBAL
where kiz.BARCODE = @kizBase64
";

            var internetOrderId = Guid.NewGuid();

            await using var connection = new SqlConnection(_connectionString);
            await connection.OpenAsync();
            await connection.ExecuteAsync(kizQuery, new
            {
                idDocument,
                documentId,
                kizBase64

            });
        }
    }

}