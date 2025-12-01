import z from 'https://cdn.jsdelivr.net/npm/@stdlib/esm@0.0.3/math/base/special/erfinv.js';
import * as Plot from "https://cdn.jsdelivr.net/npm/@observablehq/plot@0.6/+esm";

export function add_error_layer (dataset, xvar = "winter", link = Math.exp, delta = 0.05) {
  const lcl = dataset.map(row => {
    return get_lcl(0.9, row["estimate"], row["se"], link);
  });
  const lcl2 = dataset.map(row => {
    return get_lcl(0.89, row["estimate"], row["se"], link);
  });
  const ucl = dataset.map(row => {
    return get_ucl(0.9, row["estimate"], row["se"], link);
  });
  const ucl2 = dataset.map(row => {
    return get_ucl(0.89, row["estimate"], row["se"], link);
  });
  const x0 = dataset.map(row => {
    return row[xvar] - delta * 5;
  });
  const x1 = dataset.map(row => {
    return row[xvar] - delta / 2;
  });
  const x2 = dataset.map(row => {
    return row[xvar] + delta / 2;
  });
  const x3 = dataset.map(row => {
    return row[xvar] + delta * 5;
  });
  const tip_y = dataset.map(row => {
    return link(row["estimate"]);
  });
  return [
    Plot.rect(
      dataset,
      {
        x1: x1, x2: x2, y1: lcl, y2: ucl, fill: "smoothing"
      }
    ),
    Plot.rect(
      dataset,
      {
        x1: x0, x2: x3, y1: lcl, y2: lcl2, fill: "smoothing"
      }
    ),
    Plot.rect(
      dataset,
      {
        x1: x0, x2: x3, y1: ucl2, y2: ucl, fill: "smoothing"
      }
    ),
    Plot.dot(
      dataset,
      {
        x: xvar, y: tip_y, fill: "smoothing", r: 4, stroke: null
      }
    ),
    Plot.tip(
      dataset,
      Plot.pointerX({
        x: xvar, y: tip_y, fontSize: 12, stroke: "smoothing", fill: "white",
        title: (d) => `${d.type}\n${d.winter}\n${d.label}`, anchor: "middle"
      })
    )
  ]
}

export function add_fan_layer (index, dataset, link = Math.exp) {
  const lcl = dataset.map(row => {
    return get_lcl(index / 100, row["estimate"], row["se"], link);
  });
  const ucl = dataset.map(row => {
    return get_ucl(index / 100, row["estimate"], row["se"], link);
  });
  return Plot.area(
    dataset,
    {
      x1: "winter", y1: lcl, y2: ucl, fill: "smoothing",
      opacity: 1 / 18
    }
  )
}

export function add_fan_rect_layer (
  dataset, xvar = "centre_end", link = Math.exp, delta = 1,
  threshold = Math.log(0.75)
) {
  dataset = dataset.map(
    row => {
      return {
        ...row,
        x1: row[xvar] - delta / 2,
        x2: row[xvar] + delta / 2,
        lcl: get_lcl(0.9, row["estimate"], row["se"], link),
        ucl: get_ucl(0.9, row["estimate"], row["se"], link)
      };
    }
  ).map(
    row => {
      return {...row, interpretation: classification(row["lcl"], row["ucl"], threshold)};
    }
  )
  return [
    Plot.rect(
      dataset,
      {
        x1: "x1", x2: "x2", y1: "lcl", y2: "ucl", fill: "url(#gradient)"
      },
    ),
    Plot.tip(
      dataset,
      Plot.pointerX({
        x: xvar, y: "estimate", fontSize: 12, anchor: "middle", fill: "white",
        title: (d) =>
         `${d.centre_end} compared to ${d.centre_start}
${d.change}(${d.flcl};${d.fucl})\n${d.interpretation}`
      })
    )
  ]
}

export function add_legend_text (dataset, p = 0.05, link = Math.exp) {
  const legend_y = dataset.map(row => {
    return link(qnorm(p, row["estimate"], row["se"]));
  });
  return Plot.text(
    dataset,
    Plot.selectMaxX({
      x: "winter", dx: 5, y: legend_y, text: "type", frameAnchor: "middle",
      fontSize: "1em", fill: "smoothing", fontWeight: "bold",
      textAnchor: "start"
    })
  )
}

export function add_reference (dataset, xvar = "centre_end", threshold = Math.log(0.75)) {
  const xm = Math.max(...dataset.map(item => item.centre_end)) + 0.5
  const ribbon = [{
    x1: Math.min(...dataset.map(item => item.centre_end)) - 0.5,
    x2: xm, y1: threshold, y2: -threshold
  }]
  const ref_year = Math.max(...dataset.map(item => item.centre_start))
  const ref_label = [
    {x: xm, y: threshold, label: "meaningful less than " + ref_year},
    {x: xm, y: 0, label: "baseline situation " + ref_year},
    {x: xm, y: -threshold, label: "meaningful more than " + ref_year},
  ]
  return [
    Plot.rect(
      ribbon, {x1: "x1", x2: "x2", y1: "y1", y2: "y2", fill: "#BAC4CD"}
    ),
    Plot.ruleY([0]),
    Plot.text(
      ref_label,
      {
        x: "x", y: "y", text: "label", lineAnchor: "bottom", dy: -3,
        textAnchor: "end", fill: "#C04384", fontWeight: "bold"
      }
    )
  ]
}

export function classification (lcl, ucl, threshold = Math.log(0.75)) {
  const reference = 0;
  if (lcl < reference) {
    if (lcl < threshold) {
      if (ucl < 0) {
        if (ucl < threshold) {
          return "strong decrease";
        } else {
          return "decrease";
        }
      } else if (ucl < -threshold) {
          return "potential decrease";
      } else {
        return "unknown";
      }
    } else if (ucl < reference) {
      return "moderate decrease";
    } else if (ucl < -threshold) {
      return "stable";
    } else {
      return "potential increase";
    }
  } else if (ucl <= -threshold) {
    return "moderate increase";
  } else if (lcl < -threshold) {
    return "increase";
  } else {
      return "strong increase"
  }
}

export function get_lcl (p, mean, sd, link = identity) {
  return link(qnorm((1 - p) / 2, mean, sd));
}

export function get_ucl (p, mean, sd, link = identity) {
  return link(qnorm(1 - (1 - p) / 2, mean, sd));
}

export function identity (value) {
  return value;
}

export function qnorm (p, mean, sd) {
  return mean + sd * Math.sqrt(2) * z(2 * p - 1);
}
