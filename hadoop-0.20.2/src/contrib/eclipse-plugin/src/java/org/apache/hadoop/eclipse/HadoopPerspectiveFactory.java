/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.eclipse;

import org.eclipse.debug.ui.IDebugUIConstants;
import org.eclipse.jdt.ui.JavaUI;
import org.eclipse.ui.IFolderLayout;
import org.eclipse.ui.IPageLayout;
import org.eclipse.ui.IPerspectiveFactory;
import org.eclipse.ui.console.IConsoleConstants;

/**
 * Creates links to the new MapReduce-based wizards and views for a MapReduce
 * perspective
 * 
 */

public class HadoopPerspectiveFactory implements IPerspectiveFactory {

  public void createInitialLayout(IPageLayout layout) {
    layout.addNewWizardShortcut("org.apache.hadoop.eclipse.NewDriverWizard");
    layout.addNewWizardShortcut("org.apache.hadoop.eclipse.NewMapperWizard");
    layout
        .addNewWizardShortcut("org.apache.hadoop.eclipse.NewReducerWizard");

    IFolderLayout left =
        layout.createFolder("org.apache.hadoop.eclipse.perspective.left",
            IPageLayout.LEFT, 0.2f, layout.getEditorArea());
    left.addView("org.eclipse.ui.navigator.ProjectExplorer");

    IFolderLayout bottom =
        layout.createFolder("org.apache.hadoop.eclipse.perspective.bottom",
            IPageLayout.BOTTOM, 0.7f, layout.getEditorArea());
    bottom.addView(IPageLayout.ID_PROBLEM_VIEW);
    bottom.addView(IPageLayout.ID_TASK_LIST);
    bottom.addView(JavaUI.ID_JAVADOC_VIEW);
    bottom.addView("org.apache.hadoop.eclipse.view.servers");
    bottom.addPlaceholder(JavaUI.ID_SOURCE_VIEW);
    bottom.addPlaceholder(IPageLayout.ID_PROGRESS_VIEW);
    bottom.addPlaceholder(IConsoleConstants.ID_CONSOLE_VIEW);
    bottom.addPlaceholder(IPageLayout.ID_BOOKMARKS);

    IFolderLayout right =
        layout.createFolder("org.apache.hadoop.eclipse.perspective.right",
            IPageLayout.RIGHT, 0.8f, layout.getEditorArea());
    right.addView(IPageLayout.ID_OUTLINE);
    right.addView("org.eclipse.ui.cheatsheets.views.CheatSheetView");
    // right.addView(layout.ID); .. cheat sheet here

    layout.addActionSet(IDebugUIConstants.LAUNCH_ACTION_SET);
    layout.addActionSet(JavaUI.ID_ACTION_SET);
    layout.addActionSet(JavaUI.ID_CODING_ACTION_SET);
    layout.addActionSet(JavaUI.ID_ELEMENT_CREATION_ACTION_SET);
    layout.addActionSet(IPageLayout.ID_NAVIGATE_ACTION_SET);
    layout.addActionSet(JavaUI.ID_SEARCH_ACTION_SET);

    layout
        .addNewWizardShortcut("org.eclipse.jdt.ui.wizards.NewPackageCreationWizard");
    layout
        .addNewWizardShortcut("org.eclipse.jdt.ui.wizards.NewClassCreationWizard");
    layout
        .addNewWizardShortcut("org.eclipse.jdt.ui.wizards.NewInterfaceCreationWizard");
    layout
        .addNewWizardShortcut("org.eclipse.jdt.ui.wizards.NewEnumCreationWizard");
    layout
        .addNewWizardShortcut("org.eclipse.jdt.ui.wizards.NewAnnotationCreationWizard");
    layout
        .addNewWizardShortcut("org.eclipse.jdt.ui.wizards.NewSourceFolderCreationWizard");
    layout
        .addNewWizardShortcut("org.eclipse.jdt.ui.wizards.NewSnippetFileCreationWizard");
    layout.addNewWizardShortcut("org.eclipse.ui.wizards.new.folder");
    layout.addNewWizardShortcut("org.eclipse.ui.wizards.new.file");
    layout
        .addNewWizardShortcut("org.eclipse.ui.editors.wizards.UntitledTextFileWizard");

    // CheatSheetViewerFactory.createCheatSheetView().setInput("org.apache.hadoop.eclipse.cheatsheet");
  }

}
