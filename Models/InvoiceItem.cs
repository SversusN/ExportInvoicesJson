using System;

namespace ExportInvoicesJson
{
    public class InvoiceItem
    {
        public Guid Id { get; set; }
        public string ProductName { get; set; }
        public decimal? RetailPrice { get; set; }
        public decimal? SupplierPrice { get; set; }
        public string SerialNumber { get; set; }
        public DateTime? ExpirationDate { get; set; }
        /// <summary>
        /// Количество в строке накладной.
        /// </summary>
        public decimal Quantity { get; set; }
        /// <summary>
        /// Количество КИЗ в БД для строки накладной.
        /// </summary>
        public decimal KizQuantity { get; set; }
        /// <summary>
        /// Признак маркированной позиции.
        /// </summary>
        public bool Kiz { get; set; }

        /// <summary>
        /// Список КИЗ данной позиции приходной накладной в формате base64.
        /// </summary>
        public string KizList { get; set; }

        public string BaseGtin { get; set; }
    }
}