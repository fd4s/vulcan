const repoUrl = "https://github.com/fd4s/vulcan";

const apiUrl = `/vulcan/api/vulcan/index.html`;

// See https://docusaurus.io/docs/site-config for available options.
const siteConfig = {
  title: "Vulcan",
  tagline: "Functional Avro for Scala",
  url: "https://fd4s.github.io/vulcan",
  baseUrl: "/vulcan/",

  customDocsPath: "docs/target/mdoc",

  projectName: "vulcan",
  organizationName: "fd4s",

  headerLinks: [
    { href: apiUrl, label: "API Docs" },
    { doc: "overview", label: "Documentation" },
    { href: repoUrl, label: "GitHub" }
  ],

  headerIcon: "img/vulcan.white.svg",
  titleIcon: "img/vulcan.svg",
  favicon: "img/favicon.png",

  colors: {
    primaryColor: "#122932",
    secondaryColor: "#153243"
  },

  copyright: `Copyright © 2019-${new Date().getFullYear()} OVO Energy Limited.`,

  highlight: { theme: "github" },

  onPageNav: "separate",

  separateCss: ["api"],

  cleanUrl: true,

  repoUrl,

  apiUrl
};

module.exports = siteConfig;
