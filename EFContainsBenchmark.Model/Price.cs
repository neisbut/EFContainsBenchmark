using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;
using System.Text;

namespace EFContainsBenchmark.Model
{
    public class Price
    {
        public int PriceId { get; set; }

        public int SecurityId { get; set; }

        public DateTime TradedOn { get; set; }

        public int PriceSourceId { get; set; }

        public decimal ClosePrice { get; set; }

        public decimal OpenPrice { get; set; }

        public Security Security { get; set; }

        public PriceSource PriceSource { get; set; }

    }
}
