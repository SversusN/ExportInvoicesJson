using System;

namespace ExportInvoicesJson
{
    public record InvoiceItemDto
    {
        public string Id { get; init; }
        public string ProductName { get; init; }
        public decimal? RetailPrice { get; init; }
        public decimal? SupplierPrice { get; init; }
        public string? SerialNumber { get; init; }
        public DateTime? ExpirationDate { get; init; }
        public decimal Quantity { get; init; }
        public decimal? KizQuantity { get; init; }
        public bool Kiz { get; init; }
        public string? KizList { get; init; }
        public string BaseGtin { get; set; }
    }
}