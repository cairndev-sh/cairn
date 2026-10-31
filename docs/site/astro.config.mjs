import { defineConfig } from 'astro/config';
import starlight from '@astrojs/starlight';
import starlightImageZoom from 'starlight-image-zoom';
import astroMermaid from 'astro-mermaid';

// The user docs, served at https://docs.cairndev.sh: its own Vercel project
// with root directory docs/site. The site lives in the public repository
// (ADR-0121 D4), so a docs fix and the code change it describes land in one
// pull request. The marketing landing is a separate deployment at cairndev.sh.
//
// Docs editorial rule: document ONLY what is built and verified in this
// repository — no roadmap, no "coming soon". Every command and YAML snippet is
// checked against the engine before it lands here. A feature that is part of
// cairn-platform is named only to say so.

// Old slugs → the closest page in the current set. Every page that existed
// before the 2026-10 rewrite resolves to something.
const redirects = {
  '/architecture': '/',
  '/ask': '/',
  '/sdk': '/',
  '/concepts/operators': '/concepts/capabilities/',
  '/concepts/profiles-environment': '/concepts/capabilities/',
  '/concepts/packs': '/guides/write-a-pipeline/',
  '/concepts/runs-provenance': '/concepts/runs/',
  '/configuration': '/install/',
  '/database': '/install/',
  '/deployment': '/install/',
  '/installation': '/install/',
  '/runtimes': '/install/',
  '/services': '/install/',
  '/console': '/quickstart/',
  '/governance': '/concepts/gates/',
  '/modelops': '/reference/catalog/',
  '/pipelines': '/concepts/pipelines/',
  '/triggers': '/concepts/pipelines/',
  '/reference/http-api': '/reference/cli/',
  // The feature and combination-guide pages of the site before the rewrite.
  '/features': '/',
  '/features/pipelines': '/concepts/pipelines/',
  '/features/gates': '/concepts/gates/',
  '/features/dataflow-and-artifacts': '/concepts/flows/',
  '/features/governance': '/concepts/gates/',
  '/features/triggers-and-events': '/concepts/pipelines/',
  '/features/models-and-agents': '/concepts/flows/',
  '/features/monitor-and-hooks': '/reference/operators/',
  '/features/training': '/reference/catalog/',
  '/features/remote-and-secrets': '/guides/connect-a-provider/',
  '/features/backends': '/install/',
  '/features/recovery': '/concepts/runs/',
  '/features/workers': '/reference/cli/',
  '/guides/data-to-assistant': '/reference/catalog/',
  '/guides/nightly-ops': '/reference/catalog/',
  '/guides/governed-finetune': '/reference/catalog/',
  '/guides/recovery': '/concepts/runs/',
};

export default defineConfig({
  site: 'https://docs.cairndev.sh',
  redirects,
  markdown: {
    shikiConfig: {
      excludeLangs: ['mermaid'],
    },
  },
  integrations: [
    astroMermaid({
      // One palette in both site themes: diagrams sit on a paper card
      // (see custom.css), so labels are dark-on-light everywhere.
      theme: 'base',
      autoTheme: false,
      enableLog: false,
      mermaidConfig: {
        fontFamily: 'system-ui, -apple-system, "Segoe UI", sans-serif',
        flowchart: { useMaxWidth: false, curve: 'basis', padding: 12, nodeSpacing: 36, rankSpacing: 40 },
        sequence: { useMaxWidth: false },
        themeVariables: {
          fontSize: '15px',
          background: '#f7f8f2',
          primaryColor: '#ffffff',
          primaryTextColor: '#16382f',
          primaryBorderColor: '#234d3c',
          secondaryColor: '#d9f687',
          secondaryTextColor: '#16382f',
          secondaryBorderColor: '#234d3c',
          tertiaryColor: '#eef1e6',
          tertiaryTextColor: '#16382f',
          tertiaryBorderColor: '#9fb09a',
          lineColor: '#566a61',
          textColor: '#16382f',
          mainBkg: '#ffffff',
          nodeBorder: '#234d3c',
          clusterBkg: '#eef1e6',
          clusterBorder: '#9fb09a',
          titleColor: '#16382f',
          edgeLabelBackground: '#f7f8f2',
          actorBkg: '#ffffff',
          actorBorder: '#234d3c',
          actorTextColor: '#16382f',
          signalColor: '#16382f',
          signalTextColor: '#16382f',
          noteBkgColor: '#d9f687',
          noteTextColor: '#16382f',
          noteBorderColor: '#234d3c',
        },
      },
    }),
    starlight({
      plugins: [starlightImageZoom()],
      title: 'cairn',
      logo: {
        light: './src/assets/logo-light.svg',
        dark: './src/assets/logo-dark.svg',
        alt: '',
        replacesTitle: false,
      },
      favicon: '/favicon.svg',
      description:
        'cairn is a self-hosted control plane for the work behind AI models: it decides when each step may run, holds spend to a budget, pauses for data checks and people, and records what happened.',
      customCss: ['./src/styles/custom.css'],
      social: [
        {
          icon: 'github',
          label: 'GitHub',
          href: 'https://github.com/cairndev-sh/cairn',
        },
      ],
      sidebar: [
        // Cross-link to the marketing landing — a separate deployment.
        { label: '← Home', link: 'https://cairndev.sh', attrs: { target: '_self' } },
        {
          label: 'Start',
          items: [
            { label: 'What is cairn', slug: '' },
            { label: 'Quickstart', slug: 'quickstart' },
            { label: 'Install and deploy', slug: 'install' },
          ],
        },
        {
          label: 'Concepts',
          items: [
            { label: 'Pipelines, legs and steps', slug: 'concepts/pipelines' },
            { label: 'Flows', slug: 'concepts/flows' },
            { label: 'Capabilities and environments', slug: 'concepts/capabilities' },
            { label: 'Gates and approvals', slug: 'concepts/gates' },
            { label: 'Cost and budgets', slug: 'concepts/cost' },
            { label: 'Runs and the record', slug: 'concepts/runs' },
          ],
        },
        {
          label: 'Guides',
          items: [
            { label: 'Fine-tune on one machine', slug: 'guides/fine-tune-on-one-machine' },
            { label: 'Fine-tune a model on a GPU box', slug: 'guides/fine-tune-a-model' },
            { label: 'Run your first pipeline', slug: 'guides/first-pipeline' },
            { label: 'Write a pipeline', slug: 'guides/write-a-pipeline' },
            { label: 'Write a flow', slug: 'guides/write-a-flow' },
            { label: 'Connect a provider', slug: 'guides/connect-a-provider' },
            { label: 'Set up your team', slug: 'guides/team' },
          ],
        },
        {
          label: 'Reference',
          items: [
            { label: 'CLI', slug: 'reference/cli' },
            { label: 'Pipeline and flow YAML', slug: 'reference/yaml' },
            { label: 'Operators', slug: 'reference/operators' },
            { label: 'Pipeline catalog', slug: 'reference/catalog' },
          ],
        },
      ],
    }),
  ],
});
