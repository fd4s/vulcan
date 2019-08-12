package vulcan

import org.apache.avro.Schema

final class SortOrderSpec extends BaseSpec {
  describe("SortOrder") {
    it("asJava") {
      assert(SortOrder.Ascending.asJava == Schema.Field.Order.ASCENDING)
      assert(SortOrder.Descending.asJava == Schema.Field.Order.DESCENDING)
      assert(SortOrder.Ignore.asJava == Schema.Field.Order.IGNORE)
    }

    it("toString") {
      assert(SortOrder.Ascending.toString() == "Ascending")
      assert(SortOrder.Descending.toString() == "Descending")
      assert(SortOrder.Ignore.toString() == "Ignore")
    }
  }
}
