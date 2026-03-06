import { fmt } from "../../utils/number";

export default function ProducersTab({ producerReasons }) {
  return (
    <section className="dq-panel">
      <p className="dq-note">
        Producer identity is not currently exposed by DriftQ API. This tab uses real broker counters from <code>/metrics</code>.
      </p>
      <h3>Produce Rejections by Reason</h3>
      <table>
        <thead>
          <tr>
            <th>Reason</th>
            <th className="right">Count</th>
          </tr>
        </thead>
        <tbody>
          {
            producerReasons.map((r) => (
              <tr key={r.reason}>
                <td>{r.reason}</td>
                <td className="right amber">{fmt(r.value)}</td>
              </tr>
            ))
          }
          {
            producerReasons.length === 0 ? (
              <tr>
                <td colSpan={2}>no rejection metrics yet</td>
              </tr>
            ) : null
          }
        </tbody>
      </table>
    </section>
  );
}
