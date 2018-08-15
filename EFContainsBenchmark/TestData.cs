using EFContainsBenchmark.Model;
using System;
using System.Collections.Generic;
using System.Text;

namespace EFContainsBenchmark
{
    class TestData
    {

        public string Description { get; set; }

        public string Ticker { get; set; }

        public int PriceSourceId { get; set; }

        public DateTime TradedOn { get; set; }

        public decimal Price { get; set; }

        public Price PriceObject { get; set; }

    }
}
