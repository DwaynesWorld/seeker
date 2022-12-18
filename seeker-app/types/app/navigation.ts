import { ReactElement } from "react";

export interface NavigationContent {
  sections: NavigationSection[];
}

export interface NavigationSection {
  title: string;
  items: NavigationItem[];
}

export interface NavigationItem {
  label: string;
  icon: (selected: boolean) => ReactElement;
  link: string;
}
