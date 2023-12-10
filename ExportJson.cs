using System.Configuration;
using System.Text;
using System.Text.Encodings.Web;
using System.Text.Json;
using System.Text.Unicode;


namespace ExportInvoicesJson
{
    public partial class ExportJson : Form
    {
        public ExportJson()
        {
            InitializeComponent();
        }

        private static readonly string? v = ConfigurationManager.AppSettings.GetValues("folder").FirstOrDefault();
        private string? _folder = v;

        private static readonly JsonSerializerOptions options = new JsonSerializerOptions()
        {
            AllowTrailingCommas = true,
            PropertyNamingPolicy = JsonNamingPolicy.CamelCase,
            Encoder = JavaScriptEncoder.Create(UnicodeRanges.All), // Вот эта строка Вам поможет с кодировкой
            WriteIndented = true
        };

        private async void button1_Click(object sender, EventArgs e)
        {
            var connection = new InvoiceRepository();
            var invoices = await connection.FindAsync("");
            List<InvoiceDto> invoicesDto = new();

            if (invoices != null)
            {
                foreach (var invoice in invoices)
                {
                    var items = await connection.GetItemsAsync(invoice.Id);
                    var invoiceJson = InvoiceDto.FromInvoice(invoice, items);
                    invoicesDto.Add(invoiceJson);
                }
            }
            InvoicesDto invoicesDtos = new()
            {
                invoices = invoicesDto
            };


            string json = JsonSerializer.Serialize(invoicesDtos, options);

            string? path = Path.GetDirectoryName(Application.ExecutablePath);
            try { File.WriteAllText($"{_folder ?? path}/invoices.json", json, Encoding.UTF8); }
            catch (Exception ex)
            {
                MessageBox.Show(ex.Message);
            }
            ;
        }

        private void ExportJson_Load(object sender, EventArgs e)
        {

        }
    }
}