using System;
using System.ComponentModel.DataAnnotations;

namespace ExportInvoicesJson
{
    public class InvoiceItemKiz
    {
        [Required] public Guid ItemId { get; set; }

        [Required] public string KizBase64 { get; set; }
    }
}
