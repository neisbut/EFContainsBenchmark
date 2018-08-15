using System;
using System.Collections.Generic;
using System.Text;

namespace EFContainsBenchmark.Model
{
    public class SharedQueryModel
    {
        public string Ticker { get; set; }

        public int PriceSourceId { get; set; }

        public DateTime TradedOn { get; set; }

    }
}
