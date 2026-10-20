type Labels = Record<string, string>;

const escapeLabel = (value: string): string => value.replace(/\\/g, '\\\\').replace(/\n/g, '\\n').replace(/"/g, '\\"');

const labelsOf = (labels: Labels): string =>
  Object.keys(labels).length === 0
    ? ''
    : `{${Object.entries(labels)
        .map(([key, value]) => `${key}="${escapeLabel(value)}"`)
        .join(',')}}`;

/** A scrape-local registry: counters are snapshots of existing process stats, gauges come from shared state. */
export function createMetrics() {
  const families = new Map<string, { help: string; type: 'counter' | 'gauge' | 'histogram'; samples: string[] }>();

  const define = (name: string, help: string, type: 'counter' | 'gauge' | 'histogram'): void => {
    if (!families.has(name)) {
      families.set(name, { help, type, samples: [] });
    }
  };

  const sample = (
    name: string,
    help: string,
    type: 'counter' | 'gauge' | 'histogram',
    value: number | bigint,
    labels: Labels = {},
    suffix = '',
  ): void => {
    const family = families.get(name) ?? { help, type, samples: [] };
    family.samples.push(`${name}${suffix}${labelsOf(labels)} ${value}`);
    families.set(name, family);
  };

  return {
    define,
    sample,
    text: () =>
      [...families]
        .map(([name, f]) => `# HELP ${name} ${f.help}\n# TYPE ${name} ${f.type}\n${f.samples.join('\n')}\n`)
        .join(''),
  };
}

const buckets = [0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10, Infinity];

/** One bounded histogram: labels are registered route patterns and HTTP status, never account paths. */
export function createHttpMetrics() {
  const observations = new Map<
    string,
    { route: string; status: string; sum: number; count: number; buckets: number[] }
  >();

  return {
    observe(route: string, status: number, seconds: number) {
      const key = `${route}:${status}`;

      const row = observations.get(key) ?? {
        route,
        status: String(status),
        sum: 0,
        count: 0,
        buckets: buckets.map(() => 0),
      };

      row.count += 1;
      row.sum += seconds;

      for (const [i, bound] of buckets.entries()) {
        if (seconds <= bound) {
          row.buckets[i] += 1;
        }
      }

      observations.set(key, row);
    },
    write(registry: ReturnType<typeof createMetrics>) {
      const name = 'http_request_duration_seconds';
      const help = 'HTTP request duration in seconds.';
      registry.define(name, help, 'histogram');

      for (const row of observations.values()) {
        const labels = { route: row.route, status: row.status };

        for (const [i, bound] of buckets.entries()) {
          registry.sample(
            name,
            help,
            'histogram',
            row.buckets[i],
            { ...labels, le: bound === Infinity ? '+Inf' : String(bound) },
            '_bucket',
          );
        }

        registry.sample(name, help, 'histogram', row.sum, labels, '_sum');
        registry.sample(name, help, 'histogram', row.count, labels, '_count');
      }
    },
  };
}
