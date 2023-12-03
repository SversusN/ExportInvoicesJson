using System;

namespace ExportInvoicesJson

{
    public class Invoice
    {
        public Guid Id { get; set; }
        public string DocNumber { get; set; }
        public DateTime DocDate { get; set; }
        public string SupplierNumber { get; set; }
        public DateTime? SupplierDate { get; set; }
        public string SupplierName { get; set; }
        public decimal SupplierSum { get; set; }
    }
}