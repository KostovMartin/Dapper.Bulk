using System;
using System.Collections.Generic;
using System.Text;

namespace Dapper.Bulk.Tests.Attributes
{
    public class WriteAttribute : Attribute
    {
        public bool Write { get; set; }

        public WriteAttribute(bool write)
        {
            Write = write;
        }
    }
}
