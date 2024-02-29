using System;
using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;

namespace DatabaseSync.Entities
{
    [Table("AuditLog_20230101")]
    public class AuditLog
    {
        public int? AccountId { get; set; } // TODO very rarely NULL. Ask for clarification. 26 rows to be exact.

        public int? PUser_Id { get; set; }

        public int? ImpersonatedUser_Id { get; set; }

        public byte Type { get; set; }

        [MaxLength(128)]
        public string Table { get; set; }

        [MaxLength]
        public string Log { get; set; }

        public DateTime Created { get; set; }
    }
}
