using System;
using System.Collections.Generic;

namespace ExportInvoicesJson
{
    public record InvoiceDto
    {
        public string Id { get; init; }
        public string DocNumber { get; init; }
        public DateTime DocDate { get; init; }
        public string SupplierNumber { get; init; }
        public DateTime? SupplierDate { get; init; }
        public string SupplierName { get; init; }
        public decimal SupplierSum { get; init; }
        public List<InvoiceItemDto> Items { get; init; } = new();
        
        public static InvoiceDto FromInvoice(Invoice invoice, IEnumerable<InvoiceItem> items)
        {
            if (invoice == null) throw new ArgumentNullException(nameof(invoice));
            if (items == null) throw new ArgumentNullException(nameof(items));
            
            var dto = new InvoiceDto
            {
                Id = invoice.Id.ToString(),
                DocNumber = invoice.DocNumber,
                DocDate = invoice.DocDate,
                SupplierNumber = invoice.SupplierNumber,
                SupplierDate = invoice.SupplierDate,
                SupplierName = invoice.SupplierName,
                SupplierSum = invoice.SupplierSum
            };

            foreach (var item in items)
            {
                var itemDto = new InvoiceItemDto
                {
                    Id = item.Id.ToString(),
                    ProductName = item.ProductName,
                    RetailPrice = item.RetailPrice,
                    SupplierPrice = item.SupplierPrice,
                    SerialNumber = item.SerialNumber,
                    ExpirationDate = item.ExpirationDate,
                    Quantity = item.Quantity,
                    KizQuantity = item.KizQuantity,
                    Kiz = item.Kiz,
                    KizList = item.KizList,
                    BaseGtin = item.BaseGtin,
                };
                
                dto.Items.Add(itemDto);
            }

            return dto;
        }
    }
}